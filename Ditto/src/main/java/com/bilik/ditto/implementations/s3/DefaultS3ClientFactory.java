package com.bilik.ditto.implementations.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.util.Properties;

public class DefaultS3ClientFactory extends S3ClientFactory {

    public DefaultS3ClientFactory(Properties properties) {
        super(properties);
    }

    @Override
    protected S3Client createS3Client(S3ClientBuilder builder) {
        return builder.build();
    }

}
