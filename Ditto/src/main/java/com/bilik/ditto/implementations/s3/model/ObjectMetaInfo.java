package com.bilik.ditto.implementations.s3.model;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class ObjectMetaInfo {

    private String bucket;
    private String key;

    private Map<String, String> userData = new HashMap<>();

    private final Map<String, Object> metaData = new HashMap<>();

    private final Map<String, Type> metaDataValueType = new HashMap<>();

    private String contentType;

    private Instant creationTime;

    private Instant lastModified;

    private Long size;

    public ObjectMetaInfo() {}

    public ObjectMetaInfo(Map<String, String> userData, String bucket, String key) {
        if (userData != null) {
            this.userData = userData;
        }
        this.bucket = bucket;
        this.key = key;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Map<String, String> getUserData() {
        return userData;
    }

    public Map<String, Object> getMetaData() {
        return metaData;
    }

    public void putMetaData(String key, Object value, Type type) {
        metaData.put(key, value);
        metaDataValueType.put(key, type);
    }

    public Map<String, Type> getMetaDataValueType() {
        return metaDataValueType;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Instant creationTime) {
        this.creationTime = creationTime;
    }

    public Instant getLastModified() {
        return lastModified;
    }

    public void setLastModified(Instant lastModified) {
        this.lastModified = lastModified;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "ObjectMetaInfo{" +
                "bucket='" + bucket + '\'' +
                ", key='" + key + '\'' +
                ", userData=" + userData +
                ", metaData=" + metaData +
                ", metaDataValueType=" + metaDataValueType +
                ", contentType='" + contentType + '\'' +
                ", creationTime=" + creationTime +
                ", lastModified=" + lastModified +
                ", size=" + size +
                '}';
    }
}
