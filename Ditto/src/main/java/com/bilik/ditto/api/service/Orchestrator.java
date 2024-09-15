package com.bilik.ditto.api.service;

import com.bilik.ditto.api.domain.dao.entity.JobDao;
import com.bilik.ditto.api.domain.dao.repository.JobIdSequencer;
import com.bilik.ditto.api.domain.dao.repository.JobRepository;
import com.bilik.ditto.api.domain.dto.JobDescriptionDto;
import com.bilik.ditto.api.domain.internal.JobDescriptionInternal;
import com.bilik.ditto.api.endpoint.errorHandling.NotFoundException;
import com.bilik.ditto.api.service.providers.ConverterProvider;
import com.bilik.ditto.api.service.providers.HdfsResolver;
import com.bilik.ditto.api.service.providers.KafkaResolver;
import com.bilik.ditto.api.service.providers.LocalFsResolver;
import com.bilik.ditto.api.service.providers.S3Resolver;
import com.bilik.ditto.api.service.providers.DittoTypeResolver;
import com.bilik.ditto.core.common.ErrorEventProducingUncaughtExceptionHandler;
import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.concurrent.SinkProducersThreadFactory;
import com.bilik.ditto.core.concurrent.threadCommunication.QueueWorkerEventCommunicator;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventCommunicator;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;
import com.bilik.ditto.core.configuration.DittoConfiguration;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.core.job.ConvertingJobOrchestration;
import com.bilik.ditto.core.job.JobOrchestration;
import com.bilik.ditto.core.job.MonotonousJobOrchestration;
import com.bilik.ditto.core.job.input.Source;
import com.bilik.ditto.core.job.output.Sink;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@Service
public class Orchestrator {

    private static final Logger log = LoggerFactory.getLogger(Orchestrator.class);
    private static final int DEFAULT_WAITING_QUEUE = 10;

    private final Counter requestedJobs;
    private final Counter executedJobs;
    private final AtomicInteger queuedJobs;
    private final AtomicInteger runningJobsGauge;
    private final Timer initializationTimer;

    private final DittoConfiguration dittoConfiguration;
    private final DittoTypeResolver dittoTypeResolver;
    private final KafkaResolver kafkaResolver;
    private final S3Resolver s3Resolver;
    private final LocalFsResolver localFsResolver;
    private final HdfsResolver hdfsResolver;

    private final JobRepository jobRepository;
    private final JobIdSequencer seqProvider;
    private final JdbcAggregateTemplate jdbcAggregateTemplate;

    private final Map<String, JobOrchestration> runningJobs = new ConcurrentHashMap<>();
    private final BlockingQueue<JobOrchestration> jobQueue = new LinkedBlockingDeque<>(DEFAULT_WAITING_QUEUE);

    private final Consumer<JobOrchestration> jobFinishedFunction = this::jobFinished;

    private final ExecutorService queuedJobExecutor = Executors.newSingleThreadExecutor();

    // order guarantee
    private final Lock deployLock = new ReentrantLock(true);

    @Autowired
    public Orchestrator(DittoConfiguration dittoConfiguration,
                        DittoTypeResolver dittoTypeResolver,
                        KafkaResolver kafkaResolver,
                        S3Resolver s3Resolver,
                        LocalFsResolver localFsResolver,
                        HdfsResolver hdfsResolver,
                        MeterRegistry meterRegistry,
                        JobRepository jobRepository,
                        JobIdSequencer seqProvider,
                        JdbcAggregateTemplate jdbcAggregateTemplate) {
        this.dittoConfiguration = dittoConfiguration;
        this.dittoTypeResolver = dittoTypeResolver;
        this.kafkaResolver = kafkaResolver;
        this.s3Resolver = s3Resolver;
        this.localFsResolver = localFsResolver;
        this.hdfsResolver = hdfsResolver;
        this.jobRepository = jobRepository;
        this.jdbcAggregateTemplate = jdbcAggregateTemplate;

        this.requestedJobs = Counter.builder("requested_jobs")
                .description("Total job requests")
                .register(meterRegistry);
        this.executedJobs = Counter.builder("executed_jobs")
                .description("Total number of executed jobs")
                .register(meterRegistry);
        this.queuedJobs = meterRegistry
                .gauge("queued_jobs", new AtomicInteger(0));
        this.runningJobsGauge = meterRegistry
                .gauge("currently_running_jobs", new AtomicInteger(0));
        this.initializationTimer = Timer
                .builder("orchestration_initialization_time")
                .description("Duration of orchestration's initialization")
                .register(meterRegistry);
        this.seqProvider = seqProvider;
    }

    /**
     * @return jobId
     */
    public String runNewJob(JobDescriptionDto descriptionDto) {
        JobOrchestration<?, ?> jobOrchestration;
        String jobId = String.valueOf(seqProvider.nextVal());
        log.info("Establihsing new job with ID = {}", jobId);
        requestedJobs.increment();

        deployLock.lock();
        try {
            // if there is no slot and job should not be queued - immediately return
            if (runningJobs.size() == dittoConfiguration.getMaxJobsRunning() && !descriptionDto.isShallBeQueued()) {
                throw DittoRuntimeException.of("Job can not be deployed now, because max number of running jobs has been reached. Use 'shallBeQueued' flag if you want to put your job in queue");
            }
            // Here we can be sure that either there is slot for a job or,
            // if there is no slot, then job want to be queued. So initialize job and add to queue
            jobOrchestration = createOrchestration(descriptionDto, jobId);
            saveOrchestration(jobOrchestration, descriptionDto);

            if (runningJobs.size() == dittoConfiguration.getMaxJobsRunning()) {
                boolean added = jobQueue.add(jobOrchestration);
                if (!added) {
                    throw new DittoRuntimeException("Job could not be added to queue, probably because queue is already full[" + jobQueue.size() + " elements]. Try again later");
                }
                queuedJobs.incrementAndGet();
                log.info("Job {} has been added to queue. Queue size is {}", jobOrchestration.getJobId(), jobQueue.size());
                return jobOrchestration.getJobId();
            } else {
                initializationTimer.record(
                        jobOrchestration::initializeOrchestration);
                runningJobs.put(jobOrchestration.getJobId(), jobOrchestration);
            }
        } finally {
            deployLock.unlock();
            log.info("Deploy lock release at runNewJob");
        }

        // no need to block here. Job has been added to running jobs map
        // if run fails, it will trigger jobFinished() method.
        // synchronizing this method would just block ending another job
        runOrchestration(jobOrchestration);
        updateExecutedOrhcestration(jobOrchestration, false);
        return jobOrchestration.getJobId();
    }

    public void terminateJob(String jobId) {
        JobOrchestration<?, ?> jobOrchestration = runningJobs.get(jobId);
        if (jobOrchestration == null) {
            throw new NotFoundException("Job with ID " + jobId + " has not been found between currently running jobs");
        }
        deployLock.lock();
        try {
            jobOrchestration.stop();
            jobRepository.addError(jobId, "Terminated");
        } catch (Exception e) {
            log.error("Error while terminating job {}", jobId);
            throw new RuntimeException(e);
        } finally {
            deployLock.unlock();
            log.info("Deploy lock release at terminateJob");
        }
    }

    /**
     * Method is called by eventLoop thread when obOrchestration is finished.
     * Remove finished job and start next in queue if present
     */
    public void jobFinished(JobOrchestration<?,?> finishedJob) {
        JobOrchestration<?, ?> jobOrchestration;
        deployLock.lock();
        try {
            log.info("Job {} has ended", finishedJob.getJobId());
            updateFinishedOrchestration(finishedJob);

            runningJobs.remove(finishedJob.getJobId());
            runningJobsGauge.decrementAndGet();

            jobOrchestration = jobQueue.poll();
            if (jobOrchestration != null) {
                log.info("Found job {} in queue. There is still {} in queue", jobOrchestration.getJobId(), jobQueue.size());
                initializationTimer.record(
                        jobOrchestration::initializeOrchestration);
                runningJobs.put(jobOrchestration.getJobId(), jobOrchestration);
                queuedJobs.decrementAndGet();
            }
        } finally {
            deployLock.unlock();
            log.info("Deploy lock release at jobFinished");
        }
        // no need to block this part
        if (jobOrchestration != null) {
            // so we don't call it from a thread of finished orchestration
            queuedJobExecutor.execute(() -> {
                runOrchestration(jobOrchestration);
                updateExecutedOrhcestration(jobOrchestration, true);
            });
        }
    }

    private void runOrchestration(JobOrchestration<?, ?> jobOrchestration) {
        jobOrchestration.run();
        runningJobsGauge.incrementAndGet();
        executedJobs.increment();
    }

    private JobOrchestration createOrchestration(JobDescriptionDto descriptionDto, String jobId) {
        log.info("Creating orchestration for provided Job Description [{}]", descriptionDto);
        // if there is no slot and job should not be queued - immediatelly return
        if (runningJobs.size() == dittoConfiguration.getMaxJobsRunning() && !descriptionDto.isShallBeQueued()) {
            throw DittoRuntimeException.of("Job can not be deployed now, because max number of running jobs has been reached. Use 'shallBeQueued' flag if you want to put your job in queue");
        }
        if (descriptionDto.getParallelism() < 1) {
            throw new DittoRuntimeException("Parallelism of Job orchestration has to be in interval <1;notTooHigh>. Desired parallelism was " + descriptionDto.getParallelism());
        }
        // Here we can be sure that either there is slot for a job or,
        // if there is no slot, tje job want to be queued. So initialize job
        JobDescriptionInternal descriptionInternal = dtoDescriptionToInternal(descriptionDto);
        descriptionInternal.setJobId(jobId);

        // communicator for source/sink workers
        WorkerEventCommunicator workerEventCommunicator = new QueueWorkerEventCommunicator();

        Source source = switch (descriptionInternal.getSourceGeneral().getSourceType()) {
            case KAFKA -> kafkaResolver.createSource(
                    descriptionInternal, workerEventCommunicator);
            case LOCAL_FS -> localFsResolver.createSource(
                    descriptionInternal, workerEventCommunicator);
            case S3 -> s3Resolver.createSource(
                    descriptionInternal, workerEventCommunicator);
            case HDFS -> hdfsResolver.createSource(
                    descriptionInternal, workerEventCommunicator);
        };

        ThreadPoolExecutor executor = createSinkThreadPoolExecutor(descriptionInternal, workerEventCommunicator);
        Sink sink = switch (descriptionInternal.getSinkGeneral().getSinkType()) {
            case KAFKA -> kafkaResolver.createSink(
                    descriptionInternal,
                    workerEventCommunicator,
                    executor);
            case LOCAL_FS -> localFsResolver.createSink(
                    descriptionInternal,
                    workerEventCommunicator,
                    executor);
            case S3 -> s3Resolver.createSink(
                    descriptionInternal,
                    workerEventCommunicator,
                    executor);
            case HDFS -> hdfsResolver.createSink(
                    descriptionInternal,
                    workerEventCommunicator,
                    executor);
        };

        if (source.getType().dataType().equals(sink.getType().dataType())) {
            log.info("Source data type [{}] and sink data type [{}] are same, so MonotonousJobOrchestration is created", source.getType(), sink.getType());
            return new MonotonousJobOrchestration(
                    source,
                    sink,
                    dittoConfiguration.getOrchestrationConfig(),
                    workerEventCommunicator,
                    descriptionInternal.getJobId(),
                    descriptionInternal.getParallelism(),
                    jobFinishedFunction);
        } else {
            return new ConvertingJobOrchestration(
                    source,
                    sink,
                    dittoConfiguration.getOrchestrationConfig(),
                    workerEventCommunicator,
                    descriptionInternal.getJobId(),
                    descriptionInternal.getParallelism(),
                    jobFinishedFunction,
                    ConverterProvider.getConverterSupplier(descriptionInternal));
        }
    }

    // TODO - metody pre kontrolu beziacich jobov
    public Set<String> getRunningJobs() {
        return runningJobs.keySet();
    }

    public int getMaxRunningJobs() {
        return dittoConfiguration.getMaxJobsRunning();
    }

    public int getQueuedJobs() {
        return jobQueue.size();
    }

    public int getQueueSize() {
        return DEFAULT_WAITING_QUEUE;
    }

    public int getRemainingQueueCapacity() {
        return jobQueue.remainingCapacity();
    }

    public JobOrchestration getRunningJob(String jobID) {
        return runningJobs.get(jobID);
    }

    private JobDescriptionInternal dtoDescriptionToInternal(JobDescriptionDto descriptionDto) {
        JobDescriptionInternal descriptionInternal = new JobDescriptionInternal();

        descriptionInternal.setParallelism(descriptionDto.getParallelism());
        descriptionInternal.setWorkingPath(dittoConfiguration.getWorkingPath());

        descriptionInternal.setSourceGeneral(
                dittoTypeResolver.resolveSource(descriptionDto.getSourceGeneral()));
        descriptionInternal.setSinkGeneral(
                dittoTypeResolver.resolveSink(descriptionDto.getSinkGeneral()));

        // convert implementations
        convertKafka(descriptionInternal,descriptionDto);
        convertS3(descriptionInternal,descriptionDto);
        convertLocalFS(descriptionInternal,descriptionDto);
        convertHdfs(descriptionInternal,descriptionDto);
        convertConverter(descriptionInternal,descriptionDto);

        return descriptionInternal;
    }

    private void convertKafka(JobDescriptionInternal internal, JobDescriptionDto dto) {
        if (dto.getKafkaSource() != null) {
            internal.setKafkaSource(
                    new JobDescriptionInternal.KafkaSource(
                            dto.getKafkaSource().from(),
                            dto.getKafkaSource().to(),
                            dto.getKafkaSource().rangeType(),
                            dto.getKafkaSource().cluster(),
                            dto.getKafkaSource().topic()
                    )
            );
        }

        if (dto.getKafkaSink() != null) {
            JobDescriptionInternal.KafkaSink sink = new JobDescriptionInternal.KafkaSink(
                    dto.getKafkaSink().batchSize(),
                    dto.getKafkaSink().cluster(),
                    dto.getKafkaSink().topic()
            );
            internal.setKafkaSink(sink);
        }
    }

    private void convertS3(JobDescriptionInternal internal, JobDescriptionDto dto) {
        if (dto.getS3Sink() != null) {
            JobDescriptionInternal.S3Sink sink = new JobDescriptionInternal.S3Sink(
                    dto.getS3Sink().server(),
                    dto.getS3Sink().bucket(),
                    new JobDescriptionInternal.FileSegmentator(
                            dto.getS3Sink().fileSegmentator().maxSizePerFile(),
                            dto.getS3Sink().fileSegmentator().maxElementsPerFile()
                    )
            );
            internal.setS3Sink(sink);
        }

        if (dto.getS3Source() != null) {
            JobDescriptionInternal.S3Source source = new JobDescriptionInternal.S3Source(
                    dto.getS3Source().server(),
                    dto.getS3Source().bucket(),
                    dto.getS3Source().objectNamePrefixes(),
                    dto.getS3Source().from(),
                    dto.getS3Source().to()
            );
            internal.setS3Source(source);
        }
    }

    private void convertLocalFS(JobDescriptionInternal internal, JobDescriptionDto dto) {
        if (dto.getLocalSink() != null) {
            JobDescriptionInternal.LocalSink sink = new JobDescriptionInternal.LocalSink(
                    dto.getLocalSink().dir(),
                    new JobDescriptionInternal.FileSegmentator(
                            dto.getLocalSink().fileSegmentator().maxSizePerFile(),
                            dto.getLocalSink().fileSegmentator().maxElementsPerFile()
                    )
            );
            internal.setLocalSink(sink);
        }
    }

    private void convertHdfs(JobDescriptionInternal internal, JobDescriptionDto dto) {
        if (dto.getHdfsSink() != null) {
            JobDescriptionInternal.HdfsSink sink = new JobDescriptionInternal.HdfsSink(
                    dto.getHdfsSink().cluster(),
                    dto.getHdfsSink().parentPath(),
                    new JobDescriptionInternal.FileSegmentator(
                            dto.getHdfsSink().fileSegmentator().maxSizePerFile(),
                            dto.getHdfsSink().fileSegmentator().maxElementsPerFile()
                    )
            );
            internal.setHdfsSink(sink);
        }

        if (dto.getHdfsSource() != null) {
            JobDescriptionInternal.HdfsSource source = new JobDescriptionInternal.HdfsSource(
                    dto.getHdfsSource().cluster(),
                    dto.getHdfsSource().parentPath(),
                    dto.getHdfsSource().recursive(),
                    dto.getHdfsSource().prefixes(),
                    dto.getHdfsSource().from(),
                    dto.getHdfsSource().to()
            );
            internal.setHdfsSource(source);
        }
    }

    private void convertConverter(JobDescriptionInternal internal, JobDescriptionDto dto) {
        if (dto.getSpecialConverter() != null) {
            internal.setConverter(new JobDescriptionInternal.Converter(
                    dto.getSpecialConverter().name(),
                    dto.getSpecialConverter().specificType(),
                    dto.getSpecialConverter().args()
            ));
        }
    }

    private ThreadPoolExecutor createSinkThreadPoolExecutor(
            JobDescriptionInternal internal,
            WorkerEventProducer eventProducer) {

        return new ThreadPoolExecutor(
                internal.getParallelism(),
                internal.getParallelism(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(2 * internal.getParallelism()),
                new SinkProducersThreadFactory(
                        internal.getJobId(),
                        LoggingUncaughtExceptionHandler.withChild(
                                new ErrorEventProducingUncaughtExceptionHandler(eventProducer))
                        ),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private void saveOrchestration(JobOrchestration<?,?> jobOrchestration, JobDescriptionDto descriptionDto) {
        JobDao dao = new JobDao();
        dao.setId(Long.parseLong(jobOrchestration.getJobId())); // for now, we can reuse this unique ID, because its a number as well, but that might change
        dao.setJobId(jobOrchestration.getJobId());
        dao.setParallelism(jobOrchestration.getParallelism());
        dao.setQueueSize(jobOrchestration.getConfiguration().getQueueSize());
        dao.setCreationTime(LocalDateTime.now(ZoneOffset.UTC));

        dao.setSourceType(descriptionDto.getSourceGeneral().dataType().specificType() == null ?
                descriptionDto.getSourceGeneral().dataType().type() :
                descriptionDto.getSourceGeneral().dataType().type() + "-" + descriptionDto.getSourceGeneral().dataType().specificType());
        dao.setSinkType(descriptionDto.getSinkGeneral().dataType().specificType() == null ?
                descriptionDto.getSourceGeneral().dataType().type() :
                descriptionDto.getSourceGeneral().dataType().type() + "-" + descriptionDto.getSourceGeneral().dataType().specificType());

        dao.setSourceDataType(descriptionDto.getSourceGeneral().platform());
        dao.setSinkDataType(descriptionDto.getSinkGeneral().platform());

        if (descriptionDto.getSpecialConverter() != null) {
            dao.setSpecialConverterName(descriptionDto.getSpecialConverter().name());
            dao.setSpecialConverterSubtype(descriptionDto.getSpecialConverter().specificType());
            dao.setSpecialConverterSubtype(Arrays.toString(descriptionDto.getSpecialConverter().args()));
        }
        // https://spring.io/blog/2021/09/09/spring-data-jdbc-how-to-use-custom-id-generation - Template solution
        jdbcAggregateTemplate.insert(dao);
    }

    private void updateExecutedOrhcestration(JobOrchestration<?,?> jobOrchestration, boolean wasQueued) {
        JobDao original = jobRepository.findByJobId(jobOrchestration.getJobId());

        original.setStartTime(LocalDateTime.ofInstant(jobOrchestration.getStartTime(), ZoneOffset.UTC));
        original.setParallelism(jobOrchestration.getParallelism()); // parallelism is updated at initial phase
        original.setRunningTime(LocalDateTime.ofInstant(jobOrchestration.getRunningTime(), ZoneOffset.UTC));
        original.setQueued(wasQueued);

        jobRepository.save(original);
    }

    private void updateFinishedOrchestration(JobOrchestration<?,?> jobOrchestration) {
        JobDao original = jobRepository.findByJobId(jobOrchestration.getJobId());

        if (jobOrchestration.getError() != null) {
            log.info("Job {} terminated with an error !!!", jobOrchestration.getJobId());
            original.setError(jobOrchestration.getError());
        }

        original.setFinishTime(LocalDateTime.ofInstant(jobOrchestration.getFinishTime(), ZoneOffset.UTC));
        jobOrchestration.getWorkerEventsView().forEach(original::addWorkerEvent);

        jobOrchestration.getSourceCounterAggregator()
                .getCounters().entrySet().forEach(original::addCounter);

        jobOrchestration.getSinkCounterAggregator()
                .getCounters().entrySet().forEach(original::addCounter);


        jobRepository.save(original);
    }

}
