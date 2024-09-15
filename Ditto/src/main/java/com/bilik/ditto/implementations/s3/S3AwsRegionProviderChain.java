package com.bilik.ditto.implementations.s3;

import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.providers.AwsProfileRegionProvider;
import software.amazon.awssdk.regions.providers.AwsRegionProviderChain;
import software.amazon.awssdk.regions.providers.SystemSettingsRegionProvider;

import java.util.function.Supplier;

/**
 * AWS Region provider that looks for the region in this order:
 * <ol>
 *   <li>Check the 'aws.region' system property for the region.</li>
 *   <li>Check the 'AWS_REGION' environment variable for the region.</li>
 *   <li>Check the {user.home}/.aws/credentials and {user.home}/.aws/config files for the region.</li>
 * </ol>
 *
 * Same as DefaultAwsRegionProviderChain but it does not check EC2 instance
 */
public class S3AwsRegionProviderChain extends AwsRegionProviderChain {

    private S3AwsRegionProviderChain(final Builder builder) {
        super(new SystemSettingsRegionProvider(),
                new AwsProfileRegionProvider(builder.profileFile, builder.profileName));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Supplier<ProfileFile> profileFile;
        private String profileName;

        private Builder() {}

        public Builder profileFile(final Supplier<ProfileFile> profileFile) {
            this.profileFile = profileFile;
            return this;
        }

        public Builder profileName(final String profileName) {
            this.profileName = profileName;
            return this;
        }

        public S3AwsRegionProviderChain build() {
            return new S3AwsRegionProviderChain(this);
        }
    }
}