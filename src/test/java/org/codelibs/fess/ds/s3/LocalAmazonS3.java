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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.codelibs.core.io.ResourceUtil;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;

import io.minio.MinioClient;
import io.minio.PutObjectOptions;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;

public class LocalAmazonS3 {

    private static LocalAmazonS3 instance;

    static final String TEST_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE";
    static final String TEST_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    static final String TEST_REGION = "us-east-1";

    static final String[] BUCKETS = { "fess-0", "fess-1" };
    static final String[] PATHS = { "files/sample-0.txt", "files/sample-1.txt" };
    static final Map<String, String> FILE_MAP = new LinkedHashMap<>() {
        {
            for (final String path : PATHS) {
                try {
                    put(path, IOUtils.toString(ResourceUtil.getResourceAsStream(path), StandardCharsets.UTF_8));
                } catch (final IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    };

    @SuppressWarnings("rawtypes")
    private final GenericContainer container;

    private LocalAmazonS3() {
        container = new GenericContainer<>("minio/minio") //
                .withEnv("MINIO_ACCESS_KEY", TEST_ACCESS_KEY) //
                .withEnv("MINIO_SECRET_KEY", TEST_SECRET_KEY) //
                .withExposedPorts(9000) //
                .withCommand("server /data");
        container.start();
    }

    public static LocalAmazonS3 getInstance() {
        if (instance != null) {
            return instance;
        }
        return instance = new LocalAmazonS3();
    }

    public void initializeBuckets() throws Exception {
        resetBuckets();
        final MinioClient client = getMinioClient();
        for (final String bucketName : BUCKETS) {
            client.makeBucket(bucketName);
            for (final String path : PATHS) {
                try {
                    final URL url = ResourceUtil.getResource(path);
                    final URLConnection conn = url.openConnection();
                    client.putObject(bucketName, path, url.openStream(), new PutObjectOptions(conn.getContentLengthLong(), -1));
                } catch (final Exception e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    public void resetBuckets() throws Exception {
        final MinioClient client = getMinioClient();
        for (final Bucket bucket : client.listBuckets()) {
            for (final Result<Item> object : client.listObjects(bucket.name())) {
                client.removeObject(bucket.name(), object.get().objectName());
            }
            client.removeBucket(bucket.name());
        }
    }

    private MinioClient getMinioClient() throws MinioException {
        return new MinioClient(getEndpoint(), TEST_ACCESS_KEY, TEST_SECRET_KEY);
    }

    public String getEndpoint() {
        final Integer mappedPort = container.getFirstMappedPort();
        Testcontainers.exposeHostPorts(mappedPort);
        return String.format("http://%s:%s", container.getContainerIpAddress(), mappedPort);
    }

    public AmazonS3Client getAmazonS3Client() {
        return new AmazonS3Client(getParams());
    }

    public Map<String, String> getParams() {
        final Map<String, String> params = new HashMap<>();
        params.put(AmazonS3Client.ACCESS_KEY_ID, TEST_ACCESS_KEY);
        params.put(AmazonS3Client.SECRET_KEY, TEST_SECRET_KEY);
        params.put(AmazonS3Client.REGION, TEST_REGION);
        params.put(AmazonS3Client.ENDPOINT, getEndpoint());
        return params;
    }

}
