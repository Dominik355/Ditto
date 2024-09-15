package com.bilik.ditto.api.endpoint;

import com.bilik.ditto.api.domain.converter.JobDaoToDto;
import com.bilik.ditto.api.domain.converter.JobOrchstrationToRunningJobInfoDto;
import com.bilik.ditto.api.domain.dao.repository.JobRepository;
import com.bilik.ditto.api.domain.dto.AvailableSlots;
import com.bilik.ditto.api.domain.dto.JobDescriptionDto;
import com.bilik.ditto.api.domain.dto.JobDto;
import com.bilik.ditto.api.domain.dto.JobExecutionResult;
import com.bilik.ditto.api.domain.dto.RunningJobInfoDto;
import com.bilik.ditto.api.endpoint.errorHandling.NotFoundException;
import com.bilik.ditto.api.service.Orchestrator;
import com.bilik.ditto.core.exception.InvalidUserInputException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/job")
public class JobOrchestrationEndpoint {

    private static final Logger log = LoggerFactory.getLogger(JobOrchestrationEndpoint.class);

    private final Orchestrator orchestrator;
    private final JobRepository jobRepository;

    @Autowired
    public JobOrchestrationEndpoint(Orchestrator orchestrator,
                                    JobRepository jobRepository) {
        this.orchestrator = orchestrator;
        this.jobRepository = jobRepository;
    }

    @PostMapping("addNewJob")
    public ResponseEntity<JobExecutionResult> newOrchestration(@RequestBody JobDescriptionDto dto) {
        log.info("JobOrchestrationEndpoint.newOrchestration = {}", dto);
        String jobId;
        try {
            jobId = orchestrator.runNewJob(dto);
        } catch (InvalidUserInputException ex) {
            // everything went fine, so 200 is returned. But user input was wrong,
            // so job was not deployed either queued and response's error is filled
            return ResponseEntity.ok()
                    .body(JobExecutionResult.error(ex.getMessage()));
        } catch (Exception ex) {
            ex.printStackTrace();
            return ResponseEntity.internalServerError()
                    .body(JobExecutionResult.error(ex.getMessage()));
        }

        if (orchestrator.getRunningJobs().contains(jobId)) {
            return ResponseEntity.ok(JobExecutionResult.executed(jobId));
        } else {
            return ResponseEntity.ok(JobExecutionResult.queued(jobId));
        }
    }

    @DeleteMapping("terminateJob")
    public ResponseEntity<Void> terminateJob(@RequestParam String jobId) {
        log.info("JobOrchestrationEndpoint.terminateJob = {}", jobId);
        orchestrator.terminateJob(jobId);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("runningJobs")
    public ResponseEntity<Collection<String>> getRunningJobs() {
        return ResponseEntity.ok(orchestrator.getRunningJobs());
    }

    @GetMapping("runningJobsDetailed")
    public ResponseEntity<Map<String, RunningJobInfoDto>> getRunningJobsDetailed() {
        return ResponseEntity.ok(
                orchestrator.getRunningJobs().stream()
                        .collect(Collectors.toMap(
                                Function.identity(),
                                jobId -> JobOrchstrationToRunningJobInfoDto.from(orchestrator.getRunningJob(jobId))
                )));
    }

    @GetMapping("runningJobInfo")
    public ResponseEntity<RunningJobInfoDto> getRunningJobInfo(String jobId) {
        var job = orchestrator.getRunningJob(jobId);
        if(job == null) {
            throw new NotFoundException("Job with id " + jobId + " has not been found as a running job");
        }
        return ResponseEntity.ok(JobOrchstrationToRunningJobInfoDto.from(job));
    }

    @GetMapping("jobInfo")
    public ResponseEntity<JobDto> getJobInfo(@RequestParam String jobId) {
        var job = jobRepository.findByJobId(jobId);
        if(job == null) {
            throw new NotFoundException("Job with id " + jobId + " has not been found");
        }
        return ResponseEntity.ok(JobDaoToDto.daoToDto(job));
    }

    @GetMapping("availableSlots")
    public ResponseEntity<AvailableSlots> getAvailableSlots() {
        return ResponseEntity.ok(new AvailableSlots(
                orchestrator.getRunningJobs().size(),
                orchestrator.getMaxRunningJobs(),
                orchestrator.getQueuedJobs(),
                orchestrator.getQueueSize(),
                orchestrator.getRemainingQueueCapacity()

        ));
    }

}
