/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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

import static org.codelibs.fess.ds.s3.LocalAmazonS3.BUCKETS;
import static org.codelibs.fess.ds.s3.LocalAmazonS3.FILE_MAP;
import static org.codelibs.fess.ds.s3.LocalAmazonS3.PATHS;
import static org.codelibs.fess.ds.s3.LocalAmazonS3.getInstance;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AmazonS3ClientTest {

    private static LocalAmazonS3 local;
    private static AmazonS3Client client;

    @BeforeClass
    public static void setUp() throws Exception {
        local = getInstance();
        local.initializeBuckets();
        client = local.getAmazonS3Client();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        local.resetBuckets();
    }

    @Test
    public void test_getBuckets() {
        final List<String> buckets = new ArrayList<>();
        client.getBuckets(bucket -> buckets.add(bucket.name()));
        assertThat(buckets, hasItems(BUCKETS));
    }

    @Test
    public void test_getObjects() {
        for (final String bucketName : BUCKETS) {
            final List<String> objects = new ArrayList<>();
            client.getObjects(bucketName, object -> objects.add(object.key()));
            assertThat(objects, hasItems(PATHS));
        }
    }

    @Test
    public void test_getObjectsMaxKeys() {
        for (final String bucketName : BUCKETS) {
            final List<String> objects = new ArrayList<>();
            client.getObjects(bucketName, 1, object -> objects.add(object.key()));
            assertThat(objects, hasItems(PATHS));
        }
    }

    @Test
    public void test_getObject() {
        for (final String bucketName : BUCKETS) {
            for (final Map.Entry<String, String> entry : FILE_MAP.entrySet()) {
                try {
                    assertEquals(entry.getValue(), IOUtils.toString(client.getObject(bucketName, entry.getKey()), StandardCharsets.UTF_8));
                } catch (final IOException e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void test_getBucketsWithFilter() {
        final List<String> buckets = new ArrayList<>();
        client.getBuckets(new String[] { "fess-0" }, bucket -> buckets.add(bucket.name()));
        assertEquals(1, buckets.size());
        assertEquals("fess-0", buckets.get(0));
    }

    @Test
    public void test_close() {
        final AmazonS3Client testClient = local.getAmazonS3Client();
        assertNotNull(testClient);
        testClient.close(); // Should not throw exception
    }

    @Test(expected = DataStoreException.class)
    public void test_missingRegion() {
        final DataStoreParams params = new DataStoreParams();
        params.put(AmazonS3Client.ACCESS_KEY_ID, "test");
        params.put(AmazonS3Client.SECRET_KEY, "test");
        // Missing REGION parameter
        new AmazonS3Client(params);
    }

    @Test(expected = DataStoreException.class)
    public void test_missingAccessKey() {
        final DataStoreParams params = new DataStoreParams();
        params.put(AmazonS3Client.REGION, "us-east-1");
        params.put(AmazonS3Client.SECRET_KEY, "test");
        // Missing ACCESS_KEY_ID parameter
        new AmazonS3Client(params);
    }

    @Test(expected = DataStoreException.class)
    public void test_missingSecretKey() {
        final DataStoreParams params = new DataStoreParams();
        params.put(AmazonS3Client.REGION, "us-east-1");
        params.put(AmazonS3Client.ACCESS_KEY_ID, "test");
        // Missing SECRET_KEY parameter
        new AmazonS3Client(params);
    }

    @Test(expected = DataStoreException.class)
    public void test_invalidProxyPort() {
        final DataStoreParams params = new DataStoreParams();
        params.put(AmazonS3Client.REGION, "us-east-1");
        params.put(AmazonS3Client.ACCESS_KEY_ID, "test");
        params.put(AmazonS3Client.SECRET_KEY, "test");
        params.put(AmazonS3Client.PROXY_HOST_PARAM, "localhost");
        params.put(AmazonS3Client.PROXY_PORT_PARAM, "invalid"); // Invalid port
        new AmazonS3Client(params);
    }

    @Test(expected = DataStoreException.class)
    public void test_proxyHostWithoutPort() {
        final DataStoreParams params = new DataStoreParams();
        params.put(AmazonS3Client.REGION, "us-east-1");
        params.put(AmazonS3Client.ACCESS_KEY_ID, "test");
        params.put(AmazonS3Client.SECRET_KEY, "test");
        params.put(AmazonS3Client.PROXY_HOST_PARAM, "localhost");
        // Missing PROXY_PORT_PARAM
        new AmazonS3Client(params);
    }

    @Test
    public void test_maxCachedContentSize() {
        final DataStoreParams params = local.getParams();
        params.put(AmazonS3Client.MAX_CACHED_CONTENT_SIZE, "2097152"); // 2MB
        try (final AmazonS3Client testClient = new AmazonS3Client(params)) {
            assertNotNull(testClient);
            // Client should be created successfully with custom maxCachedContentSize
        }
    }

    @Test
    public void test_customEndpoint() {
        final DataStoreParams params = local.getParams();
        assertNotNull(params.getAsString(AmazonS3Client.ENDPOINT));
        try (final AmazonS3Client testClient = new AmazonS3Client(params)) {
            assertNotNull(testClient);
            assertEquals(params.getAsString(AmazonS3Client.ENDPOINT), testClient.getEndpoint());
        }
    }

    @Test
    public void test_getRegion() {
        assertNotNull(client.getRegion());
        assertEquals("us-east-1", client.getRegion().id());
    }

    @Test
    public void test_getObjectsPagination() {
        // Test that pagination works correctly with maxKeys
        for (final String bucketName : BUCKETS) {
            final List<String> objectsWithPagination = new ArrayList<>();
            client.getObjects(bucketName, 1, object -> objectsWithPagination.add(object.key()));

            final List<String> objectsWithoutPagination = new ArrayList<>();
            client.getObjects(bucketName, 1000, object -> objectsWithoutPagination.add(object.key()));

            // Both should return the same objects
            assertEquals(objectsWithoutPagination.size(), objectsWithPagination.size());
            assertTrue(objectsWithPagination.containsAll(objectsWithoutPagination));
        }
    }

}
