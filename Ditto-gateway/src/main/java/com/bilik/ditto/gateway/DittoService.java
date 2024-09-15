package com.bilik.ditto.gateway;

import com.bilik.ditto.gateway.clients.DittoClient;
import com.bilik.ditto.gateway.clients.KubeClient;
import com.bilik.ditto.gateway.dto.AvailableSlots;
import com.bilik.ditto.gateway.dto.ConnectivityDto;
import com.bilik.ditto.gateway.dto.ConverterDto;
import com.bilik.ditto.gateway.dto.JobDescriptionDto;
import com.bilik.ditto.gateway.dto.JobDto;
import com.bilik.ditto.gateway.dto.JobExecutionResult;
import com.bilik.ditto.gateway.dto.RunningJobInfoDto;
import com.bilik.ditto.gateway.server.RestException;
import com.bilik.ditto.gateway.server.objects.HttpStatus;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DittoService {

    private final RoundRobin roundRobin;

    private KubeClient kubeClient;
    private final DittoClient dittoClient;

    public DittoService(DittoClient dittoClient, KubeClient kubeClient) {
        this.roundRobin =  new RoundRobin();
        this.dittoClient = dittoClient;
        this.kubeClient = kubeClient;
    }

    public Map<String, Set<String>> getRunningJobs() {
        return dittoClient.aggregateRunningJobs(getRunningPodsIps());
    }

    public Map<String, Map<String, RunningJobInfoDto>> getRunningJobsDetailed() {
        return dittoClient.aggregateRunningJobsDetailed(getRunningPodsIps());
    }

    public void terminateJob(String jobId) {
        dittoClient.terminateJobAggregated(getRunningPodsIps(), jobId);
    }

    public RunningJobInfoDto getRunningJobInfo(String jobId) {
        return dittoClient.getRunningJobInfoAggregated(getRunningPodsIps(), jobId);
    }

    public Map<String, AvailableSlots> getAvailableSlots() {
        return dittoClient.getAvailableSlotsAggregated(getRunningPodsIps());
    }

    public Map<String, ConnectivityDto> getConnectivityInfo() {
        return dittoClient.getConnectivityInfoAggregated(getRunningPodsIps());
    }

    /**
     * The only synchronized method, so we are sure there won't be any race condition
     */
    public synchronized JobExecutionResult newOrchestration(JobDescriptionDto dto) {
        String address = findInstanceForNewJob(dto.isShallBeQueued());
        return dittoClient.newOrchestration(address, dto)
                .join().getBody().get(); // do not handle optional, if there is error in ditto instance it will be rethrown as forwardingException
    }

    public JobDto getJobInfo(String jobId) {
        return dittoClient.getJobInfo(getNextIP(), jobId)
                .join().getBody().orElseThrow(
                        () -> new RestException("Job with ID " + jobId + " has not been found", HttpStatus.NOT_FOUND));
    }

    public List<String> getTopics(String cluster) {
        return dittoClient.getTopics(getNextIP(), cluster)
                .join().getBody().orElse(Collections.emptyList());
    }

    public List<String> getBuckets(String cluster) {
        return dittoClient.getBuckets(getNextIP(), cluster)
                .join().getBody().orElse(Collections.emptyList());
    }

    public List<String> getKafkaClusters() {
        return dittoClient.getKafkaClusters(getNextIP())
                .join().getBody().orElse(Collections.emptyList());
    }

    public List<String> getS3Instances() {
        return dittoClient.getS3Instances(getNextIP())
                .join().getBody().orElse(Collections.emptyList());
    }

    public List<String> getHdfsClusters() {
        return dittoClient.getHdfsClusters(getNextIP())
                .join().getBody().orElse(Collections.emptyList());
    }

    public Map<String, List<String>> getSupportedTypes() {
        return dittoClient.getSupportedTypes(getNextIP())
                .join().getBody().orElse(Collections.emptyMap());
    }

    public List<String> getSupportedProtoTypes() {
        return dittoClient.getSupportedProtoTypes(getNextIP())
                .join().getBody().orElse(Collections.emptyList());
    }

    public List<ConverterDto> getConverters() {
        return dittoClient.getConverters(getNextIP())
                .join().getBody().orElse(Collections.emptyList());
    }

    public List<String> platforms() {
        return dittoClient.platforms(getNextIP())
                .join().getBody().orElse(Collections.emptyList());
    }

    public String threadDump(String ip) {
        return dittoClient.threadDump(ip)
                .join().getBody().orElse("no threads? wut?");
    }

    public List<String> getRunningPodsIps() {
        var ips = kubeClient.getRunningPodIps();
        if (ips == null || ips.isEmpty()) {
            throw new RuntimeException("No available instances");
        }
        return ips;
    }

    /**
     * Returns the address of one of the ditto instances.
     * For each call, it finds the currently running instances
     * and uses RoundRobin to select another ip address.
     * Used for calling stateless methods, where particular instance
     * does not matter to us.
     */
    public String getNextIP() {
        return roundRobin.next(getRunningPodsIps());
    }

    /**
     * Deploy new Job to :
     * 1. instance with lowest num of running jobs
     * 2. if no slot available in any instance, then to instance
     *      with the most empty queue (if shallBeQueued is true)
     * 3. throw exception if even no queue space
     */
    private String findInstanceForNewJob(boolean shallBeQueued) {
        var slots = getAvailableSlots();

        // 1
        Map.Entry<String, Integer> instanceWithLowestRunningJobs = Pair.of(null, 0);
        for (var entry : slots.entrySet()) {
            var available = entry.getValue().availableExecutionSlots();
            if (available > 0 && available > instanceWithLowestRunningJobs.getValue()) {
                instanceWithLowestRunningJobs = Pair.of(entry.getKey(), entry.getValue().availableExecutionSlots());
            }
        }

        // 2
        if (instanceWithLowestRunningJobs.getKey() == null) {
            if (!shallBeQueued) {
                throw new RuntimeException("There is currently no space to run another job. You can try to allow queueing");
            }
            Map.Entry<String, Integer> instanceWithLowestQueuedJobs = Pair.of(null, 0);
            for (var entry : slots.entrySet()) {
                var available = entry.getValue().availableQueueSlots();
                if (available > 0 && available > instanceWithLowestQueuedJobs.getValue()) {
                    instanceWithLowestQueuedJobs = Pair.of(entry.getKey(), entry.getValue().availableQueueSlots());
                }
            }

            if (instanceWithLowestQueuedJobs.getKey() != null) {
                return instanceWithLowestQueuedJobs.getKey();
            }

        } else {
            return instanceWithLowestRunningJobs.getKey();
        }

        throw new RuntimeException("here is currently no space to run another job. All slots and queues are full");
    }

    /**
     * supply max in every call, so it's resistant to changing the number of replicas
     */
    public static class RoundRobin {
        private volatile int current;

        public synchronized int next(int max) {
            return current = (current + 1) % max;
        }

        public <T> T next(List<T> list) {
            return list.get(next(list.size()));
        }
    }
}
