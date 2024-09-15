package com.bilik.ditto.implementations.s3;

import com.bilik.ditto.core.common.collections.Tuple2;
import com.bilik.ditto.core.exception.DittoRuntimeException;
import com.bilik.ditto.implementations.s3.model.DeletedObjects;
import com.bilik.ditto.implementations.s3.model.ObjectMetaInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static software.amazon.awssdk.core.sync.ResponseTransformer.toBytes;
import static software.amazon.awssdk.core.sync.ResponseTransformer.toFile;
import static software.amazon.awssdk.core.sync.ResponseTransformer.toInputStream;

public class S3Handler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(S3Handler.class);

    private final S3Client s3Client;

    public S3Handler(Properties properties, URI uri) {
        s3Client = new DefaultS3ClientFactory(properties).getS3Client(uri);
    }

    public Optional<ObjectMetaInfo> getObjectMetaInfo(String bucket, String path) {
        log.info("getObjectMetaInfo() called for  bucket [{}] and path [{}]", bucket, path);
        checkParameter(bucket);
        var request = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();
        try {
            var response = s3Client.headObject(request);

            ObjectMetaInfo metaInfo = new ObjectMetaInfo(response.metadata(), bucket, path);
            metaInfo.setLastModified(response.lastModified());
            metaInfo.setContentType(response.contentType());
            metaInfo.setSize(response.contentLength());

            metaInfo.putMetaData("versionId", response.versionId(), String.class);
            metaInfo.putMetaData("expires", response.expires(), Instant.class);
            metaInfo.putMetaData("expiration", response.expiration(), String.class);
            metaInfo.putMetaData("eTag", response.eTag(), String.class);
            metaInfo.putMetaData("contentEncoding", response.contentEncoding(), String.class);

            return Optional.of(metaInfo);
        } catch (NoSuchKeyException ex) {
            log.info("Bucket {} has no key {}", bucket, path);
            return Optional.empty();
        } catch (Exception ex) {
            throw new DittoRuntimeException("Exception occured in method getObjectMetaInfo for bucket " + bucket + " and path " + path, ex);
        }
    }

    public List<Bucket> listBuckets() {
        log.info("listBuckets() called");
        try {
            return s3Client.listBuckets().buckets();
        } catch (Exception ex) {
            log.error("List bucket(s) failed: {}", ex.getMessage());
            throw new DittoRuntimeException(ex);
        }
    }

    /**
     * Why return iterator ? Because this if paging iterator. S3 allows to fetch max 1000 objects per request.
     * This iterator automatically makes another calls for us. So it iterate over all the records.
     * Listing all the objects with listObjects() and then iterating over all of them might be costly.
     * So for use-cases, where buffering all the data first is not needed, this might be preferable solution.
     */
    public Iterator<ListObjectsV2Response> listObjectsIterator(String bucket, String prefix) {
        log.info("listObjectsIterator() called for  bucket [{}] and prefix [{}]", bucket, prefix);
        checkParameter(bucket);
        try {
            return s3Client.listObjectsV2Paginator(builder -> builder
                    .bucket(bucket)
                    .prefix(prefix)
                    .build())
                    .iterator();
        } catch (NoSuchKeyException ex) {
            log.info("Bucket {} has no key {}", bucket, prefix);
            return Collections.emptyIterator();
        } catch (Exception ex) {
            log.error("List Object(s) on [{}] in Bucket [{}] Failed: {}", prefix, bucket, ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    public List<S3Object> listObjects(String bucket, String prefix, Range<Long> range) {
        log.info("listObjects() called for  bucket [{}] and path [{}]", bucket, prefix);
        checkParameter(bucket);
        try {
            List<S3Object> objects = new ArrayList<>();

            ListObjectsV2Iterable iterable = s3Client.listObjectsV2Paginator(builder -> builder
                    .bucket(bucket)
                    .prefix(prefix)
                    .build());

            Iterator<ListObjectsV2Response> iterator =  iterable.iterator();
            while(iterator.hasNext()) {
                for (S3Object object : iterator.next().contents()) {
                    if (range != null) {
                        if (!range.contains(object.lastModified().toEpochMilli())) {
                            continue;
                        }
                    }
                    objects.add(object);
                }
            }

            log.info("List Object(s) with Prefix [{}] in Bucket [{}] Succesfully executed. Found [{}] item(s)", prefix, bucket, objects.size());
            return objects;
        } catch (NoSuchKeyException ex) {
            log.info("Bucket {} has no key {}", bucket, prefix);
            return new ArrayList<>(0);
        } catch (Exception ex) {
            log.error("List Object(s) on [{}] in Bucket [{}] Failed: {}", prefix, bucket, ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    public String mkdir(String bucket, String path) {
        checkParameter(bucket);
        try {
            var response = s3Client.putObject(
                    builder -> builder.bucket(bucket)
                            .key(path.endsWith("/") ? path : path + '/')
                            .tagging("Directory"),
                    RequestBody.empty());
            return response.eTag();
        } catch (Exception err) {
            log.error("Create directory [{}] in Bucket [{}] Failed: {}", path, bucket, err.getMessage());
            throw new DittoRuntimeException(err);
        }
    }

    public boolean exists(String bucket, String path) {
        checkParameter(bucket);
        try {
            var response = s3Client.headObject(builder ->
                    builder.bucket(bucket).key(path));
            return response != null;
        } catch(NoSuchKeyException ex) {
            return false;
        } catch (Exception ex) {
            log.error("Check Object [{}] Existence in Bucket [{}] Failed: {}", path, bucket, ex.getMessage());
            throw new DittoRuntimeException(ex);
        }
    }

    public boolean exists(String bucket) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucket)
                .build();
        try {
            s3Client.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    // Predefined download variants
    /**
     * This input stream should be closed to release the underlying connection back to the connection pool.
     */
    public ResponseInputStream<GetObjectResponse> downloadToInputStream(String bucket, String objectKey) {
        return download(bucket, objectKey, toInputStream());
    }

    public GetObjectResponse downloadToFile(String bucket, String objectKey, Path localFile) {
        return download(bucket, objectKey, toFile(localFile));
    }

    public ResponseBytes<GetObjectResponse> downloadToBytes(String bucket, String objectKey) {
        return download(bucket, objectKey, toBytes());
    }

    public <ReturnT> ReturnT download(String bucket, String objectKey, ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer) {
        checkParameter(bucket);
        try {
            log.debug("Downloading object {}", objectKey);
            ReturnT response = s3Client.getObject(
                    builder -> builder
                            .bucket(bucket)
                            .key(objectKey),
                    responseTransformer);
            log.info("Downloading Object [{}] From Bucket [{}]: {}", objectKey, bucket, response == null ? "Failed" : "Success");

            return response;
        } catch (Exception ex) {
            log.error("Download Object [{}] From Bucket [{}] Failed: {}", objectKey, bucket, ex.getMessage());
            throw new DittoRuntimeException(ex);
        }
    }

    @SafeVarargs
    public final boolean upload(String bucket, String objectKey, InputStream is, Tuple2<String, String>... metaDataPairs) throws IOException {
        return upload(
                bucket,
                objectKey,
                RequestBody.fromByteBuffer(ByteBuffer.wrap(IOUtils.toByteArray(is))),
                metaDataPairs);
    }

    @SafeVarargs
    public final boolean upload(String bucket, String objectKey, Path path, Tuple2<String, String>... metaDataPairs) {
        return upload(
                bucket,
                objectKey,
                RequestBody.fromFile(path),
                metaDataPairs);
    }

    @SafeVarargs
    public final boolean upload(String bucket, String objectKey, byte[] byteArr, Tuple2<String, String>... metaDataPairs) {
        return upload(
                bucket,
                objectKey,
                RequestBody.fromBytes(byteArr),
                metaDataPairs);
    }

    /**
     * @return true if upload was successful
     */
    @SafeVarargs
    public final boolean upload(String bucket, String objectKey, RequestBody requestBody, Tuple2<String, String>... metaDataPairs) {
        log.info("Upload Object in Bucket [{}] and objectKey [{}]", bucket, objectKey);
        checkParameter(bucket);
        Objects.requireNonNull(requestBody, "Invalid Inputstream");

        var request = genPutObjectRequest(bucket, objectKey, metaDataPairs);
        try {
            var response = s3Client.putObject(request, requestBody);
            return response != null;

        } catch (Exception ex) {
            log.error("Upload Streaming File to Bucket [{}] Path={} Failed: {}", bucket, objectKey, ex.getMessage());
            throw new DittoRuntimeException(ex);
        }
    }

    public boolean deleteObject(String bucket, String path) {
        log.info("Delete Object [{}] in Bucket [{}]", path, bucket);
        checkParameter(bucket);
        try {
            var response = s3Client.deleteObject(
                    builder -> builder.bucket(bucket).key(path));
            log.info("Delete Object [{}] in Bucket [{}] returned delete marker [{}]", path, bucket, response.deleteMarker());
            return response.deleteMarker();
        } catch (Exception ex) {
            log.error("Delete Object [{}] in Bucket [{}] Failed: {}", path, bucket, ex.getMessage());
            throw new DittoRuntimeException(ex);
        }
    }

    public DeletedObjects emptyBucket(String bucket) {
        log.info("Emptying bucket [{}]", bucket);
        try {
            List<ObjectIdentifier> identifiers =
                    s3Client.listObjectVersions(builder -> builder.bucket(bucket))
                            .versions()
                            .stream()
                            .map(version ->
                                    ObjectIdentifier.builder()
                                            .key(version.key())
                                            .versionId(version.versionId())
                                            .build())
                            .collect(Collectors.toList());
            log.info("Found {} objects in bucket {}", identifiers.size(), bucket);

            var response =
                    s3Client.deleteObjects(
                            DeleteObjectsRequest.builder()
                                    .bucket(bucket)
                                    .delete(
                                            Delete.builder()
                                                    .objects(identifiers)
                                                    .build())
                                    .build());

            return DeletedObjects.fromDeleteObjectsResponse(response);
        } catch (Exception ex) {
            log.error("Empty Bucket [{}] Failed: {}", bucket, ex.getMessage());
            throw new DittoRuntimeException(ex);
        }
    }

    public boolean deleteBucket(String bucket) {
        log.info("Deleting bucket [{}] with it's contents", bucket);
        DeletedObjects deletedObjects = emptyBucket(bucket);
        if (deletedObjects.errors > 0) {
            log.error("Bucket [{}] can not be deleted, because there were objects which couldn't be removed", bucket);
            return false;
        }

        try {
            s3Client.deleteBucket(builder -> builder.bucket(bucket));
        } catch (Exception ex) {
            log.error("Deleting Bucket [{}] Failed: {}", bucket, ex.getMessage());
            throw new DittoRuntimeException(ex);
        }
        return true;
    }

    @Override
    public void close() {
        s3Client.close();
    }

    private PutObjectRequest genPutObjectRequest(String bucket, String key, Tuple2<String, String>... metaData) {
        PutObjectRequest.Builder builder = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key);

        if (metaData != null && metaData.length > 0) {
            builder.metadata(
                    Arrays.stream(metaData)
                            .filter(m -> !StringUtils.isAnyEmpty(m.getT1(), m.getT2()))
                            .collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2))
            );
        }

        return builder.build();
    }

    private void checkParameter(final String bucket) {
        if (StringUtils.isBlank(bucket)) {
            throw new IllegalArgumentException("Invalid Bucket Name");
        } else if (this.s3Client == null) {
            throw new IllegalStateException("Client Has Not Been Initialized");
        }
    }

}
