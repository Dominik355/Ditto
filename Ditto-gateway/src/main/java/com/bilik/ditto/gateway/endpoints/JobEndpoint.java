package com.bilik.ditto.gateway.endpoints;

import com.bilik.ditto.gateway.DittoService;
import com.bilik.ditto.gateway.dto.*;
import com.bilik.ditto.gateway.server.RequestBody;
import com.bilik.ditto.gateway.server.RequestMapping;
import com.bilik.ditto.gateway.server.RequestParam;
import com.bilik.ditto.gateway.server.objects.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JobEndpoint {

    private static final Logger log = LoggerFactory.getLogger(JobEndpoint.class);

    private final DittoService service;

    public JobEndpoint(DittoService service) {
        this.service = service;
    }
    
    @RequestMapping(method = HttpMethod.POST, value = "addNewJob")
    public JobExecutionResult newOrchestration(@RequestBody JobDescriptionDto dto) {
        log.info("JobOrchestrationEndpoint.newOrchestration = {}", dto);
        return service.newOrchestration(dto);
    }

    @RequestMapping(method = HttpMethod.DELETE, value = "terminateJob")
    public void terminateJob(@RequestParam("jobId") String jobId) {
        service.terminateJob(jobId);
    }

    @RequestMapping(method = HttpMethod.GET, value = "runningJobs")
    public Collection<String> getRunningJobs() {
        return service.getRunningJobs().values()
                .stream()
                .flatMap(Set::stream)
                .toList();
    }

    @RequestMapping(method = HttpMethod.GET, value = "runningJobsDetailed")
    public Map<String, RunningJobInfoDto> getRunningJobsDetailed() {
        return service.getRunningJobsDetailed().values()
                .stream()
                .reduce(
                        new HashMap<>(),
                        (a, b) -> {a.putAll(b);return a;},
                        (a, b) -> {a.putAll(b);return a;});
    }

    @RequestMapping(method = HttpMethod.GET, value = "runningJobInfo")
    public RunningJobInfoDto getRunningJobInfo(@RequestParam("jobId") String jobId) {
        return service.getRunningJobInfo(jobId);
    }

    @RequestMapping(method = HttpMethod.GET, value = "jobInfo")
    public JobDto getJobInfo(@RequestParam("jobId") String jobId) {
        return service.getJobInfo(jobId);
    }

    @RequestMapping(method = HttpMethod.GET, value = "availableSlots")
    public AvailableSlots getAvailableSlots() {
        return service.getAvailableSlots()
                .values()
                .stream()
                .reduce(AvailableSlots::merge)
                .get();
    }

    @RequestMapping(method = HttpMethod.GET, value = "availableSlotsByInstance")
    public Map<String, AvailableSlots> availableSlotsByInstance() {
        return service.getAvailableSlots();
    }

}