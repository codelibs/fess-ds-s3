/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.s3;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Map;
import java.util.function.Consumer;

public class AmazonS3Client implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AmazonS3Client.class);

    // parameters for authentication
    protected static final String REGION = "region";
    protected static final String ACCESS_KEY_ID = "access_key_id";
    protected static final String SECRET_KEY = "secret_key";

    // other parameters
    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    protected final Map<String, String> params;

    protected final S3Client client;
    protected final Region region;
    protected int maxCachedContentSize = 1024 * 1024;

    public AmazonS3Client(final Map<String, String> params) {
        this.params = params;
        final String size = params.get(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }

        final String region = params.getOrDefault(REGION, StringUtil.EMPTY);
        if (region.isEmpty()) {
            throw new DataStoreException("Parameter '" + REGION + "' is required");
        }
        this.region = Region.of(region);
        final AwsCredentialsProvider awsCredentialsProvider = new AwsBasicCredentialsProvider(params);
        try {
            client = S3Client.builder() //
                    .region(this.region).credentialsProvider(awsCredentialsProvider) //
                    .build();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }
    }

    public Region getRegion() {
        return region;
    }

    public void getBuckets(final Consumer<Bucket> consumer) {
        client.listBuckets().buckets().forEach(consumer);
    }

    public void getObjects(final String bucket, final Consumer<S3Object> consumer) {
        client.listObjectsV2(builder -> builder.bucket(bucket).fetchOwner(true).build()).contents().forEach(consumer);
    }

    public ResponseInputStream<GetObjectResponse> getObject(final String bucket, final String key) {
        // TODO request params
        return client.getObject(builder -> builder.bucket(bucket).key(key).build());
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    static class AwsBasicCredentialsProvider implements AwsCredentialsProvider {
        String accessKeyId;
        String secretAccessKey;

        AwsBasicCredentialsProvider(final Map<String, String> params) {
            accessKeyId = params.getOrDefault(ACCESS_KEY_ID, StringUtil.EMPTY);
            secretAccessKey = params.getOrDefault(SECRET_KEY, StringUtil.EMPTY);
            if (accessKeyId.isEmpty() || secretAccessKey.isEmpty()) {
                throw new DataStoreException("Parameter '" + //
                        ACCESS_KEY_ID + "', '" + //
                        SECRET_KEY + "' is required");
            }
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        }
    }
}
