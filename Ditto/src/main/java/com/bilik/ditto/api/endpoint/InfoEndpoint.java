package com.bilik.ditto.api.endpoint;

import com.bilik.ditto.api.domain.dto.ConnectivityDto;
import com.bilik.ditto.api.domain.dto.ConverterDto;
import com.bilik.ditto.api.service.ConnectivityChecker;
import com.bilik.ditto.api.service.providers.ConverterProvider;
import com.bilik.ditto.api.service.providers.HdfsResolver;
import com.bilik.ditto.api.service.providers.KafkaResolver;
import com.bilik.ditto.api.service.providers.S3Resolver;
import com.bilik.ditto.api.service.providers.DittoTypeResolver;
import com.bilik.ditto.core.type.SupportedPlatform;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.s3.model.Bucket;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/v1/info")
public class InfoEndpoint {

    private static final Logger log = LoggerFactory.getLogger(InfoEndpoint.class);

    private final ConnectivityChecker connectivityChecker;
    private final KafkaResolver kafkaResolver;
    private final S3Resolver s3Resolver;
    private final HdfsResolver hdfsResolver;
    private final DittoTypeResolver dittoTypeResolver;


    @Autowired
    public InfoEndpoint(ConnectivityChecker connectivityChecker,
                        KafkaResolver kafkaResolver,
                        S3Resolver s3Resolver,
                        HdfsResolver hdfsResolver,
                        DittoTypeResolver dittoTypeResolver) {
        this.connectivityChecker = connectivityChecker;
        this.kafkaResolver = kafkaResolver;
        this.s3Resolver = s3Resolver;
        this.hdfsResolver = hdfsResolver;
        this.dittoTypeResolver = dittoTypeResolver;
    }

    @GetMapping("connectivityInfo")
    public ResponseEntity<ConnectivityDto> getConnectivityInfo() {
        log.info("InfoEndpoint.getConnectivityInfo called");
        return ResponseEntity.ok(new ConnectivityDto(connectivityChecker.checkConnectivity()));
    }

    @GetMapping("topics")
    public ResponseEntity<Collection<String>> getTopics(@RequestParam String cluster) throws ExecutionException, InterruptedException {
        log.info("InfoEndpoint.getTopics called for cluster {}", cluster);
        ListTopicsOptions options = new ListTopicsOptions();
        options.timeoutMs(4_000);
        return ResponseEntity.ok(kafkaResolver.getAdminClient(cluster)
                .listTopics(options)
                .names()
                .get());
    }

    @GetMapping("buckets")
    public ResponseEntity<Collection<String>> getBuckets(@RequestParam String cluster) throws ExecutionException, InterruptedException {
        log.info("InfoEndpoint.getBuckets called for cluster {}", cluster);
        return ResponseEntity.ok(s3Resolver.getS3Handler(cluster)
                .listBuckets()
                .stream()
                .map(Bucket::name)
                .toList());
    }

    @GetMapping("kafkaClusters")
    public ResponseEntity<Collection<String>> getKafkaClusters() {
        log.info("InfoEndpoint.getKafkaClusters called");
        return ResponseEntity.ok(
                kafkaResolver.getAvailableClusters().stream()
                        .map(cluster -> cluster.getName())
                        .toList()
        );
    }

    @GetMapping("s3Instances")
    public ResponseEntity<Collection<String>> getS3Instances() {
        log.info("InfoEndpoint.getS3Instances called");
        return ResponseEntity.ok(s3Resolver.getAvailableInstances());
    }

    @GetMapping("hdfsClusters")
    public ResponseEntity<Collection<String>> getHdfsClusters() {
        log.info("InfoEndpoint.getHdfsClusters called");
        return ResponseEntity.ok(hdfsResolver.getAvailableInstances());
    }

    @GetMapping("supportedTypes")
    public ResponseEntity<Map<String, List<String>>> getSupportedTypes() {
        log.info("InfoEndpoint.getSupportedTypes called");
        return ResponseEntity.ok(dittoTypeResolver.getSupportedTypes());
    }

    @GetMapping("supportedProtoTypes")
    public ResponseEntity<Collection<String>> getSupportedProtoTypes() {
        log.info("InfoEndpoint.getSupportedProtoTypes called");
        return ResponseEntity.ok(dittoTypeResolver.getSupportedProtos());
    }

    @GetMapping("converters")
    public ResponseEntity<List<ConverterDto>> getConverters() {
        log.info("InfoEndpoint.getConverters called");
        return ResponseEntity.ok(ConverterProvider.getSupportedConverters());
    }

    @GetMapping("platforms")
    public ResponseEntity<Collection<String>> platforms() {
        log.info("InfoEndpoint.platforms called");
        return ResponseEntity.ok(
                Arrays.stream(SupportedPlatform.values())
                        .map(platform -> platform.name())
                        .toList());
    }

    @GetMapping("threadDump")
    public ResponseEntity<String> threadDump() {
        StringBuffer threadDump = new StringBuffer(System.lineSeparator());
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        ThreadInfo[] threadInfos;
        try {
            threadInfos = threadMXBean.dumpAllThreads(true, true);
        } catch (Exception e) {
            threadInfos = threadMXBean.dumpAllThreads(false, false);
        }

        for(ThreadInfo threadInfo : threadInfos) {
            threadDump.append(threadInfo.toString());
        }
        return ResponseEntity.ok(threadDump.toString());
    }

}
