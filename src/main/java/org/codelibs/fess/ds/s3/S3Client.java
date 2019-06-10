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
import software.amazon.awssdk.regions.Region;

import java.util.Map;

public class S3Client implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(S3Client.class);

    // parameters for authentication
    protected static final String REGION = "region";
    protected static final String ACCESSKEY_ID = "accesskey_id";
    protected static final String SECRET_ACCESSKEY = "secret_accesskey";

    // other parameters
    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    protected final Map<String, String> params;

    protected software.amazon.awssdk.services.s3.S3Client client;
    protected int maxCachedContentSize = 1024 * 1024;

    public S3Client(final Map<String, String> params) {
        this.params = params;
        final String size = params.get(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }

        final String region = params.getOrDefault(REGION, StringUtil.EMPTY);
        if (region.isEmpty()) {
            throw new DataStoreException("Parameter '" + REGION + "' is required");
        }
        final AwsCredentialsProvider awsCredentialsProvider = new AwsBasicCredentialsProvider(params);
        try {
            client = software.amazon.awssdk.services.s3.S3Client.builder() //
                    .region(Region.of(region)).credentialsProvider(awsCredentialsProvider) //
                    .build();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }
    }

    // TODO implement getObject, getBucket ...

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
            accessKeyId = params.getOrDefault(ACCESSKEY_ID, StringUtil.EMPTY);
            secretAccessKey = params.getOrDefault(SECRET_ACCESSKEY, StringUtil.EMPTY);
            if (accessKeyId.isEmpty() || secretAccessKey.isEmpty()) {
                throw new DataStoreException("Parameter '" + //
                        ACCESSKEY_ID + "', '" + //
                        SECRET_ACCESSKEY + "' is required");
            }
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        }
    }
}
