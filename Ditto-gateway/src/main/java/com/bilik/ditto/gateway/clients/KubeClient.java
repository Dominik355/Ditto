package com.bilik.ditto.gateway.clients;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KubeClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KubeClient.class);

    public static final String APP_LABEL = "app";
    private final static String SERVICE_DNS_TEMPLATE = "%s.%s.svc.cluster.local";

    private final KubernetesClient client;
    private final String appLabel;
    private final String serviceDnsName;

    public KubeClient(String namespace, String serviceName, String appLabel, String authToken) {
        this.appLabel = appLabel;
        this.serviceDnsName = String.format(SERVICE_DNS_TEMPLATE, serviceName, namespace);
        var configBuilder = new ConfigBuilder()
                .withNamespace(namespace)
                .withRequestTimeout(5_000);

        if (authToken != null) {
            configBuilder.withOauthToken(authToken);

        }

        this.client = new KubernetesClientBuilder()
                .withConfig(configBuilder.build())
                .build();
        log.info("Created Kubernetes client for namespace: {}", namespace);
    }

    public List<String> getRunningPodIps() {
        try {
            return getRunningPodIpsByDnsResolving();
        } catch (UnknownHostException e) {
            log.error("Ip addresses could not be resolved using plain DNS. UnknownHostException occurred. Trying to obtain ips using Kube API. Exception: {}", ExceptionUtils.getStackTrace(e));
            return getRunningPods().stream()
                    .map(pod -> pod.getStatus().getPodIP())
                    .toList();
        }
    }

    public List<String> getRunningPodIpsByDnsResolving() throws UnknownHostException {
        return Arrays.stream(InetAddress.getAllByName(serviceDnsName))
                .map(InetAddress::getHostAddress)
                .toList();
    }

    // fabric8
    public KubernetesClient getClient() {
        return this.client;
    }

    public List<PodInternal> getRunningPodInternals() {
        return getRunningPods().stream()
                .map(pod -> new PodInternal(pod.getMetadata().getName(), pod.getStatus().getPodIP()))
                .toList();
    }

    public List<Pod> getRunningPods() {
        return client.pods()
                .withLabel(APP_LABEL, appLabel)
                .list()
                .getItems()
                .stream()
                .filter(pod -> {
                    if (pod.getStatus().getContainerStatuses().size() > 1) {
                        // im just curious if pod can havem ultiple statuses, becasue official kube client does not contain List, just single element
                        log.error("Pod {} contain multiple statuses {}", pod.getMetadata().getName(), pod.getStatus().getContainerStatuses());
                    }
                    var podState = pod.getStatus().getContainerStatuses().get(0).getState();
                    return podState.getRunning() != null && podState.getTerminated() == null;
                })
                .toList();
    }

    public record PodInternal(String name, String ip) {}

    @Override
    public void close() throws IOException {
        this.client.close();
    }

}
