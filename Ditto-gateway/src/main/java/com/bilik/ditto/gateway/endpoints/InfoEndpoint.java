package com.bilik.ditto.gateway.endpoints;

import com.bilik.ditto.gateway.DittoService;
import com.bilik.ditto.gateway.dto.*;
import com.bilik.ditto.gateway.server.RequestMapping;
import com.bilik.ditto.gateway.server.RequestParam;
import com.bilik.ditto.gateway.server.objects.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class InfoEndpoint {

    private static final Logger log = LoggerFactory.getLogger(InfoEndpoint.class);

    private final DittoService service;

    public InfoEndpoint(DittoService service) {
        this.service = service;
    }

    @RequestMapping(method = HttpMethod.GET, value = "connectivityInfo")
    public Map<String, ConnectivityDto> getConnectivityInfo() {
        log.info("InfoEndpoint.getConnectivityInfo called");
        return service.getConnectivityInfo();
    }

    @RequestMapping(method = HttpMethod.GET, value = "topics")
    public Collection<String> getTopics(@RequestParam("cluster") String cluster) {
        log.info("InfoEndpoint.getTopics called for cluster {}", cluster);
        return service.getTopics(cluster);
    }

    @RequestMapping(method = HttpMethod.GET, value = "buckets")
    public Collection<String> getBuckets(@RequestParam("cluster") String cluster) {
        log.info("InfoEndpoint.getBuckets called for cluster {}", cluster);
        return service.getBuckets(cluster);
    }

    @RequestMapping(method = HttpMethod.GET, value = "kafkaClusters")
    public Collection<String> getKafkaClusters() {
        log.info("InfoEndpoint.getKafkaClusters called");
        return service.getKafkaClusters();
    }

    @RequestMapping(method = HttpMethod.GET, value = "s3Instances")
    public Collection<String> getS3Instances() {
        log.info("InfoEndpoint.getS3Instances called");
        return service.getS3Instances();
    }

    @RequestMapping(method = HttpMethod.GET, value = "hdfsClusters")
    public Collection<String> getHdfsClusters() {
        log.info("InfoEndpoint.getHdfsClusters called");
        return service.getHdfsClusters();
    }

    @RequestMapping(method = HttpMethod.GET, value = "supportedTypes")
    public Map<String, List<String>> getSupportedTypes() {
        log.info("InfoEndpoint.getSupportedTypes called");
        return service.getSupportedTypes();
    }

    @RequestMapping(method = HttpMethod.GET, value = "supportedProtoTypes")
    public Collection<String> getSupportedProtoTypes() {
        log.info("InfoEndpoint.getSupportedProtoTypes called");
        return service.getSupportedProtoTypes();
    }

    @RequestMapping(method = HttpMethod.GET, value = "converters")
    public List<ConverterDto> getConverters() {
        log.info("InfoEndpoint.getConverters called");
        return service.getConverters();
    }

    @RequestMapping(method = HttpMethod.GET, value = "platforms")
    public Collection<String> platforms() {
        log.info("InfoEndpoint.platforms called");
        return service.platforms();
    }

    @RequestMapping(method = HttpMethod.GET, value = "threadDump")
    public String threadDump(@RequestParam("instance") String instance) {
        return service.threadDump(instance);
    }

    @RequestMapping(method = HttpMethod.GET, value = "availableInstances")
    public List<String> availableInstances() {
        return service.getRunningPodsIps();
    }
}
