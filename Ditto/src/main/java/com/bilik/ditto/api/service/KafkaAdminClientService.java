package com.bilik.ditto.api.service;

import com.bilik.ditto.core.configuration.properties.KafkaProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Service;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaAdminClientService implements Closeable {

    private final Map<String, AdminClient> adminClientCache = new ConcurrentHashMap<>();
    private int adminClientTimeout = 30000;

    public AdminClient get(KafkaProperties.Cluster cluster) {
        AdminClient client = adminClientCache.get(cluster.getName());
        if (client == null) {
            Properties props = new Properties();
            props.putAll(cluster.getProperties());
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, adminClientTimeout);
            props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "ditto-admin-client-" + System.currentTimeMillis());
            client = AdminClient.create(props);
            adminClientCache.put(cluster.getName(), client);
            return client;
        }
        return client;
    }

    @Override
    public void close() {
        adminClientCache.values().forEach(AdminClient::close);
    }

}
