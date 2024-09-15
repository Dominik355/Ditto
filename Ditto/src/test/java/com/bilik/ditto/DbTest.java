package com.bilik.ditto;

import com.bilik.ditto.api.domain.dao.entity.JobDao;
import com.bilik.ditto.api.domain.dao.repository.JobRepository;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.metric.ThreadSafeCounter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@ActiveProfiles(profiles = "test")
@SpringBootTest(classes = {DittoApplication.class, IntegrationTestBase.class})
public class DbTest extends SpringExtension {

    @Autowired
    private JobRepository jobRepository;

//    @Test
    public void nana() {
        UUID id = UUID.randomUUID();
        JobDao jobDao = new JobDao();
        jobDao.setFinishTime(LocalDateTime.now());
        jobDao.setJobId(id.toString());
        jobDao.setSinkType("sink type");
        jobDao.setSinkDataType("data sink");
        jobDao.setSourceType("sour typ");
        jobDao.setSourceDataType("source d t");
        jobDao.addWorkerEvent(new WorkerEvent(WorkerEvent.WorkerState.STARTED, WorkerThread.WorkerType.SOURCE));
        jobDao.addCounter(Map.entry("key", new ThreadSafeCounter()));

        jobRepository.save(jobDao);

        System.out.println("find all");
        System.out.println(jobRepository.findAll());

        System.out.println("Found Job:");
        System.out.println(jobRepository.findByJobId(id.toString()));
    }

}
