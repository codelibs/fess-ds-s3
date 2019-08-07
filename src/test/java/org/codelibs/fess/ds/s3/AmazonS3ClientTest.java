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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import org.apache.commons.io.IOUtils;
import org.codelibs.core.io.ResourceUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.stream.Stream;

import static cloud.localstack.TestUtils.getClientS3;
import static org.codelibs.fess.ds.s3.TestUtils.getClient;
import static org.codelibs.fess.ds.s3.TestUtils.resetBuckets;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AmazonS3ClientTest {

    private static AmazonS3Client client;
    private static final String BUCKET_NAME = "fess";
    private static final String[] FILES = { "sample-0.txt", "sample-1.txt" };

    @BeforeClass
    public static void setUp() throws Exception {
        resetBuckets();
        client = getClient();

        final AmazonS3 s3 = getClientS3();
        final Bucket bucket = s3.createBucket(BUCKET_NAME);
        Stream.of(FILES).forEach(path -> {
            try {
                final File file = new File(ResourceUtil.getResource(path).toURI());
                s3.putObject(bucket.getName(), file.getName(), file);
            } catch (final Exception e) {
                fail(e.getMessage());
            }
        });
    }

    @AfterClass
    public static void tearDown() {
        resetBuckets();
    }

    @Test
    public void test_getBuckets() {
        client.getBuckets(bucket -> assertEquals(BUCKET_NAME, bucket.name()));
    }

    @Test
    public void test_getObjects() {
        final Iterator<String> itr = Stream.of(FILES).iterator();
        client.getObjects(BUCKET_NAME, object -> assertEquals(itr.next(), object.key()));
    }

    @Test
    public void test_getObject() throws IOException {
        assertEquals("hogehoge", IOUtils.toString(client.getObject(BUCKET_NAME, FILES[0]), StandardCharsets.UTF_8));
        assertEquals("hugahuga", IOUtils.toString(client.getObject(BUCKET_NAME, FILES[1]), StandardCharsets.UTF_8));
    }

}
