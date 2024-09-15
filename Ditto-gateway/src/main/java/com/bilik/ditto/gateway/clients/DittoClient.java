package com.bilik.ditto.gateway.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.bilik.ditto.gateway.dto.*;
import com.bilik.ditto.gateway.server.RestException;
import com.bilik.ditto.gateway.server.objects.HttpHeaders;
import com.bilik.ditto.gateway.server.objects.HttpResponse;
import com.bilik.ditto.gateway.server.objects.HttpStatus;
import com.bilik.ditto.gateway.server.utils.ObjectMapperFactory;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class DittoClient {

    private static final String INFO_PARENT_PATH = "/api/v1/info";
    private static final String JOB_PARENT_PATH = "/api/v1/job";
    private static final String SCHEME = "http://";
    private static final int PORT = 8080;

    private final HttpClient httpClient;
    private final ObjectWriter objectWriter;


    public DittoClient(HttpClient httpClient) {
        this.httpClient = httpClient;
        this.objectWriter = ObjectMapperFactory.writer();
    }

    // AGGREGATING CALLS

    /**
     * Aggregates running jobs across ditto instances.
     * @param ips - ip addresses of available ditto instances
     * @return Map, where key is IP address and value is a list of currently running jobs
     */
    public Map<String, Set<String>> aggregateRunningJobs(List<String> ips) {
        Map<String, CompletableFuture<HttpResponse<Set<String>>>> futures = new HashMap<>();
        for (String ip : ips) {
            futures.put(ip, getRunningJobs(ip));
        }

        Map<String, Set<String>> resultMap = new HashMap<>();
        for (var entry : futures.entrySet()) {
            resultMap.put(
                    entry.getKey(),
                    entry.getValue()
                            .join()
                            .getBody()
                            .orElseGet(HashSet::new));
        }

        return resultMap;
    }

    public Map<String, Map<String, RunningJobInfoDto>> aggregateRunningJobsDetailed(List<String> ips) {
        Map<String, CompletableFuture<HttpResponse<Map<String, RunningJobInfoDto>>>> futures = new HashMap<>();
        for (String ip : ips) {
            futures.put(ip, getRunningJobsDetailed(ip));
        }

        Map<String, Map<String, RunningJobInfoDto>> resultMap = new HashMap<>();
        for (var entry : futures.entrySet()) {
            resultMap.put(
                    entry.getKey(),
                    entry.getValue()
                            .join()
                            .getBody()
                            .orElseGet(HashMap::new));
        }

        return resultMap;
    }

    public void terminateJobAggregated(List<String> ips, String jobId) {
        var aggregatedJobs = aggregateRunningJobs(ips);
        String correctIP = null;
        for (var entry : aggregatedJobs.entrySet()) {
            if (entry.getValue().contains(jobId)) {
                correctIP = entry.getKey();
            }
        }

        if (correctIP == null) {
            throw new RestException("Job with id " + jobId + " has not been found in currently running jobs", HttpStatus.NOT_FOUND);
        }
        terminateJob(correctIP, jobId).join();
    }

    public RunningJobInfoDto getRunningJobInfoAggregated(List<String> ips, String jobId) {
        var aggregatedJobs = aggregateRunningJobs(ips);
        String correctIP = null;
        for (var entry : aggregatedJobs.entrySet()) {
            if (entry.getValue().contains(jobId)) {
                correctIP = entry.getKey();
            }
        }

        if (correctIP == null) {
            throw new RestException("Job with id " + jobId + " has not been found in currently running jobs", HttpStatus.NOT_FOUND);
        }
        // even though, we know that job is in that instance and its in running state, it could change in time between these 2 calls
        return getRunningJobInfo(correctIP, jobId).join().getBody().orElseThrow(() -> new RestException("Job with id " + jobId + " has not been found between currently running jobs", HttpStatus.NOT_FOUND));
    }

    public Map<String, ConnectivityDto> getConnectivityInfoAggregated(List<String> ips) {
        Map<String, CompletableFuture<HttpResponse<ConnectivityDto>>> futures = new HashMap<>();
        for (String ip : ips) {
            futures.put(ip, getConnectivityInfo(ip));
        }

        Map<String, ConnectivityDto> resultMap = new HashMap<>();
        for (var entry : futures.entrySet()) {
            resultMap.put(
                    entry.getKey(),
                    entry.getValue()
                            .join()
                            .getBody()
                            .orElse(null));
        }

        return resultMap;
    }

    public Map<String, AvailableSlots> getAvailableSlotsAggregated(List<String> ips) {
        Map<String, CompletableFuture<HttpResponse<AvailableSlots>>> futures = new HashMap<>();
        for (String ip : ips) {
            futures.put(ip, getAvailableSlots(ip));
        }

        Map<String, AvailableSlots> resultMap = new HashMap<>();
        for (var entry : futures.entrySet()) {
            resultMap.put(
                    entry.getKey(),
                    entry.getValue()
                            .join()
                            .getBody()
                            .orElse(null));
        }

        return resultMap;
    }

    // JOB CALLS
    public CompletableFuture<HttpResponse<JobExecutionResult>> newOrchestration(String ip, JobDescriptionDto dto) {
        var request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(toJson(dto)))
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .uri(buildURI(ip, JOB_PARENT_PATH, "/addNewJob"))
                .build();
        return httpClient.send(request, JobExecutionResult.class);
    }

    public CompletableFuture<HttpResponse<Void>> terminateJob(String ip, String jobId) {
        var request = HttpRequest.newBuilder()
                .DELETE()
                .uri(buildURI(ip, JOB_PARENT_PATH, "/terminateJob?jobId=" + jobId))
                .build();
        return httpClient.send(request, void.class);
    }

    public CompletableFuture<HttpResponse<Set<String>>> getRunningJobs(String ip) {
        return httpClient.<Set<String>>send(buildJobGetRequest(ip, "/runningJobs"), Set.class);
    }

    public CompletableFuture<HttpResponse<Map<String, RunningJobInfoDto>>> getRunningJobsDetailed(String ip) {
        return httpClient.<Map<String, RunningJobInfoDto>>send(buildJobGetRequest(ip, "/runningJobsDetailed"), Map.class);
    }

    public CompletableFuture<HttpResponse<RunningJobInfoDto>> getRunningJobInfo(String ip, String jobId) {
        return httpClient.send(buildJobGetRequest(ip, "/runningJobInfo?jobId=" + jobId), RunningJobInfoDto.class);
    }

    public CompletableFuture<HttpResponse<JobDto>> getJobInfo(String ip, String jobId) {
        return httpClient.send(buildJobGetRequest(ip, "/jobInfo?jobId=" + jobId), JobDto.class);
    }

    public CompletableFuture<HttpResponse<AvailableSlots>> getAvailableSlots(String ip) {
        return httpClient.send(buildJobGetRequest(ip, "/availableSlots"), AvailableSlots.class);
    }

    // INFO CALLS
    public CompletableFuture<HttpResponse<ConnectivityDto>> getConnectivityInfo(String ip) {
        return httpClient.send(buildInfoGetRequest(ip, "/connectivityInfo"), ConnectivityDto.class);
    }

    public CompletableFuture<HttpResponse<List<String>>> getTopics(String ip, String cluster) {
        return httpClient.<List<String>>send(buildInfoGetRequest(ip, "/topics?cluster=" + cluster), List.class);
    }

    public CompletableFuture<HttpResponse<List<String>>> getBuckets(String ip, String cluster) {
        return httpClient.<List<String>>send(buildInfoGetRequest(ip, "/buckets?cluster=" + cluster), List.class);
    }

    public CompletableFuture<HttpResponse<List<String>>> getKafkaClusters(String ip) {
        return httpClient.<List<String>>send(buildInfoGetRequest(ip, "/kafkaClusters"), List.class);
    }

    public CompletableFuture<HttpResponse<List<String>>> getS3Instances(String ip) {
        return httpClient.<List<String>>send(buildInfoGetRequest(ip, "/s3Instances"), List.class);
    }

    public CompletableFuture<HttpResponse<List<String>>> getHdfsClusters(String ip) {
        return httpClient.<List<String>>send(buildInfoGetRequest(ip, "/hdfsClusters"), List.class);
    }

    public CompletableFuture<HttpResponse<Map<String, List<String>>>> getSupportedTypes(String ip) {
        return httpClient.<Map<String, List<String>>>send(buildInfoGetRequest(ip, "/supportedTypes"), Map.class);
    }

    public CompletableFuture<HttpResponse<List<String>>> getSupportedProtoTypes(String ip) {
        return httpClient.<List<String>>send(buildInfoGetRequest(ip, "/supportedProtoTypes"), List.class);
    }

    public CompletableFuture<HttpResponse<List<ConverterDto>>> getConverters(String ip) {
        return httpClient.<List<ConverterDto>>send(buildInfoGetRequest(ip, "/converters"), List.class);
    }

    public CompletableFuture<HttpResponse<List<String>>> platforms(String ip) {
        return httpClient.<List<String>>send(buildInfoGetRequest(ip, "/platforms"), List.class);
    }

    public CompletableFuture<HttpResponse<String>> threadDump(String ip) {
        return httpClient.send(buildInfoGetRequest(ip, "/threadDump"), String.class);
    }

    private HttpRequest buildJobGetRequest(String ip, String specificPart) {
        return HttpRequest.newBuilder()
                .GET()
                .uri(buildURI(ip, JOB_PARENT_PATH, specificPart))
                .build();
    }

    private HttpRequest buildInfoGetRequest(String ip, String specificPart) {
        return HttpRequest.newBuilder()
                .GET()
                .uri(buildURI(ip, INFO_PARENT_PATH, specificPart))
                .build();
    }

    public URI buildURI(String ip, String path, String specificPart) {
        return URI.create(SCHEME + ip + ":" + PORT + path + specificPart);
    }

    private String toJson(Object obj) {
        try {
            return objectWriter.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    
}
