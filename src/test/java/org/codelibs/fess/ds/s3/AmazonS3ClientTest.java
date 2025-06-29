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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
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

    /*
    @Test
    public void test_getObjectsMaxKeys() {
        for (final String bucketName : BUCKETS) {
            final List<String> objects = new ArrayList<>();
            client.getObjects(bucketName, 1, object -> objects.add(object.key()));
            assertThat(objects, hasItems(PATHS));
        }
    }
    */

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

}
