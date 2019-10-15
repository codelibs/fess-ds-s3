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

import cloud.localstack.LocalstackTestRunner;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.codelibs.fess.ds.s3.TestUtils.*;
import static org.junit.Assert.assertEquals;

@RunWith(LocalstackTestRunner.class)
public class AmazonS3ClientTest {

    private static AmazonS3Client client;

    @BeforeClass
    public static void setUp() throws Exception {
        initializeBuckets();
        client = getClient();
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
    public void test_getObjectsMaxKeys() {
        final Iterator<String> itr = Stream.of(FILES).iterator();
        client.getObjects(BUCKET_NAME, 1, object -> assertEquals(itr.next(), object.key()));
    }

    @Test
    public void test_getObject() throws IOException {
        assertEquals("hogehoge", IOUtils.toString(client.getObject(BUCKET_NAME, FILES[0]), StandardCharsets.UTF_8));
        assertEquals("hugahuga", IOUtils.toString(client.getObject(BUCKET_NAME, FILES[1]), StandardCharsets.UTF_8));
    }

}
