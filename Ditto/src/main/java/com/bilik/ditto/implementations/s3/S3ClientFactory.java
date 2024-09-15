package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.common.DurationResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Protocol;

import java.net.URI;
import java.util.Properties;
import java.util.function.Consumer;

public abstract class S3ClientFactory {

    public static final String REGION = "s3.region";

    public static final String ACCESS_KEY = "s3.access.key";

    public static final String SECRET_KEY = "s3.secret.key";

    public static final String CONNECTION_TIMEOUT = "s3.connection.timeout";

    public static final String MAX_CONNECTIONS = "s3.max.connections";

    public static final String PROTOCOL = "s3.protocol";

    public static final String PROXY_DOMAIN = "s3.proxy.domain";

    public static final String PROXY_HOST = "s3.proxy.host";

    public static final String PROXY_PASSWORD = "s3.proxy.password";

    public static final String PROXY_PORT = "s3.proxy.port";

    public static final String PROXY_USERNAME = "s3.proxy.username";

    public static final String PROXY_WORKSTATION = "s3.proxy.workstation";

    public static final String SOCKET_TIMEOUT = "s3.socket.timeout";

    public static final String PATH_STYLE_ACCESS = "s3.path.style.access";

    public static final String PROFILE_NAME = "s3.profile.name";

    private static final Logger log = LoggerFactory.getLogger(S3ClientFactory.class);

    private static final String DEFAULT_PROTOCOL = Protocol.HTTPS.toString();

    private static final Region DEFAULT_REGION = Region.AWS_GLOBAL;
    
    private final Properties properties;

    protected S3ClientFactory(Properties properties) {
        this.properties = properties;
    }

    /**
     * Build a new Amazon S3 instance with the URI and the properties provided
     */
    public S3Client getS3Client(final URI uri) {
        S3ClientBuilder builder = getS3ClientBuilder()
                .endpointOverride(uri)
                .region(getRegion(true))
                .credentialsProvider(getCredentialsProvider())
                .httpClient(getHttpClient())
                .serviceConfiguration(getServiceConfiguration());

        return createS3Client(builder);
    }

    /**
     * should return a new S3Client
     *
     * @param builder S3ClientBuilder mandatory.
     * @return {@link S3Client}
     */
    protected abstract S3Client createS3Client(final S3ClientBuilder builder);

    protected S3ClientBuilder getS3ClientBuilder() {
        return S3Client.builder();
    }

    private URI getEndpointUri(String host, int port) {
        String scheme = getProtocol();
        return URI.create(
                port != -1 ?
                        String.format("%s://%s:%d", scheme, host, port) :
                        String.format("%s://%s", scheme, host)
        );
    }

    private String getProtocol() {
        return properties.getProperty(PROTOCOL) != null ?
                Protocol.fromValue(properties.getProperty(PROTOCOL)).toString() :
                DEFAULT_PROTOCOL;
    }

    private Region getRegion(boolean useDefault) {
        Region region = null;
        if (properties.getProperty(REGION) != null) {
            region = Region.of(properties.getProperty(REGION));
        }
        if (region == null) {
            try {
                region = getAwsRegionProvider().getRegion();
            } catch (SdkClientException e) {
                log.warn("Unable to load region from any of the providers in the chain: {}", e.getMessage());
            }
        }

        return region != null ? region : (useDefault ? DEFAULT_REGION : null);
    }

    protected AwsCredentialsProvider getCredentialsProvider() {
        return areSecretsProvided() ?
                StaticCredentialsProvider.create(getAwsCredentials()) :
                DefaultCredentialsProvider.builder()
                        .profileName(properties.getProperty(PROFILE_NAME))
                        .build();
    }

    protected SdkHttpClient getHttpClient() {
        return getApacheHttpClientBuilder().build();
    }

    private ApacheHttpClient.Builder getApacheHttpClientBuilder() {
        final ApacheHttpClient.Builder builder = ApacheHttpClient.builder();

        setProperty(property -> builder.connectionTimeout(DurationResolver.resolveDuration(property)), CONNECTION_TIMEOUT);
        setProperty(property -> builder.maxConnections(Integer.parseInt(property)), MAX_CONNECTIONS);
        setProperty(property -> builder.socketTimeout(DurationResolver.resolveDuration(property)),  SOCKET_TIMEOUT);

        return builder.proxyConfiguration(getProxyConfiguration());
    }

    protected S3Configuration getServiceConfiguration() {
        S3Configuration.Builder builder = S3Configuration.builder();
        if (properties.getProperty(PATH_STYLE_ACCESS) != null && Boolean.parseBoolean(properties.getProperty(PATH_STYLE_ACCESS))) {
            builder.pathStyleAccessEnabled(true);
        }
        return builder.build();
    }

    private AwsRegionProvider getAwsRegionProvider() {
        return S3AwsRegionProviderChain.builder()
                .profileName(properties.getProperty(PROFILE_NAME))
                .build();
    }

    protected AwsCredentials getAwsCredentials() {
        return AwsBasicCredentials.create(properties.getProperty(ACCESS_KEY), properties.getProperty(SECRET_KEY));
    }

    protected ProxyConfiguration getProxyConfiguration() {
        final ProxyConfiguration.Builder builder = ProxyConfiguration.builder();

        if (properties.getProperty(PROXY_HOST) != null) {
            final String host = properties.getProperty(PROXY_HOST);
            final String portStr = properties.getProperty(PROXY_PORT);
            int port;
            try {
                port = portStr != null ? Integer.parseInt(portStr) : -1;
            } catch (final NumberFormatException e) {
                throw new RuntimeException("Property " + PROXY_PORT + " has to be integer! Found value : " + portStr);
            }

            final URI uri = getEndpointUri(host, port);
            builder.endpoint(uri);
        }

        setProperty(builder::username, PROXY_USERNAME);
        setProperty(builder::password, PROXY_PASSWORD);
        setProperty(builder::ntlmDomain, PROXY_DOMAIN);
        setProperty(builder::ntlmWorkstation, PROXY_WORKSTATION);

        ProxyConfiguration proxy = builder.build();
        log.debug("Created proxy configuration : " + proxy);
        return proxy;
    }

    private boolean areSecretsProvided() {
        String accessKey = properties.getProperty(ACCESS_KEY);
        String secretKey = properties.getProperty(SECRET_KEY);
        if (accessKey == null || secretKey == null) {
            log.debug("Access-Key " + (accessKey != null ? " was provided [val = " + accessKey + "]" : " was not provided")  +
                    ", Secret-Key " + (secretKey != null ? " was provided" : " was not provided") +
                    ". Properties are not used, falling back to DefaultCredentialsProvider");
            return false;
        }
        log.debug("Secret keys found in properties [access key = " + accessKey +"] and will be used");
        return true;
    }

    private void setProperty(final Consumer<String> consumer, final String propertyName) {
        try {
            String value = properties.getProperty(propertyName);
            if (value != null) {
                consumer.accept(properties.getProperty(propertyName));
            }
        } catch (Exception e) {
            log.warn("The '" + propertyName + "' property could not be loaded with this value: " + properties.getProperty(propertyName), e);
        }
    }

    private void printWarningMessage(final String propertyName) {
        log.warn("The '{}' property could not be loaded with this value: {}",
                propertyName,
                properties.getProperty(propertyName));
    }

}
