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

import cloud.localstack.Localstack;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static cloud.localstack.TestUtils.*;
import static org.junit.Assert.assertEquals;

public class AmazonS3ClientTest {

    private static AmazonS3Client client;

    @BeforeClass
    public static void setUp() {
        final AmazonS3 s3 = getClientS3();
        final Bucket bucket = s3.createBucket("fess");
        s3.putObject(bucket.getName(), "test.txt", "Test Contents");

        final Map<String, String> params = new HashMap<>();
        params.put(AmazonS3Client.ACCESS_KEY_ID, TEST_ACCESS_KEY);
        params.put(AmazonS3Client.SECRET_KEY, TEST_SECRET_KEY);
        params.put(AmazonS3Client.REGION, DEFAULT_REGION);
        params.put(AmazonS3Client.ENDPOINT, Localstack.getEndpointS3());
        client = new AmazonS3Client(params);
    }

    @Test
    public void test_getBuckets() {
        client.getBuckets(bucket -> assertEquals("fess", bucket.name()));
    }

    @Test
    public void test_getObjects() {
        client.getObjects("fess", object -> assertEquals("test.txt", object.key()));
    }

    @Test
    public void test_getObject() throws IOException {
        assertEquals("Test Contents", IOUtils.toString(client.getObject("fess", "test.txt"), StandardCharsets.UTF_8));
    }

}
