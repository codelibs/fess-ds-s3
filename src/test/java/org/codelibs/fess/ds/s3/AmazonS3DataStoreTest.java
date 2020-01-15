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

import org.apache.tika.io.FilenameUtils;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lastaflute.di.core.factory.SingletonLaContainerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.codelibs.fess.ds.s3.LocalAmazonS3.*;
import static org.junit.Assert.*;

public class AmazonS3DataStoreTest {

    private static LocalAmazonS3 local;
    private static AmazonS3DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        local = getInstance();
        local.initializeBuckets();
        initializeContainer();
        dataStore = new AmazonS3DataStore();
    }

    private static void initializeContainer() {
        if (SingletonLaContainerFactory.hasContainer()) {
            SingletonLaContainerFactory.destroy();
            SingletonLaContainerFactory.setExternalContext(null);
        }
        SingletonLaContainerFactory.setConfigPath("test_app.xml");
        SingletonLaContainerFactory.init();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        local.resetBuckets();
        ComponentUtil.setFessConfig(null);
    }

    @Test
    public void test_getObjectMap() {
        final AmazonS3Client client = local.getAmazonS3Client();
        client.getBuckets(bucket -> {
            client.getObjects(bucket.name(), object -> {
                try {
                    final String url = dataStore.getUrl(local.getEndpoint(), TEST_REGION, bucket.name(), object.key());
                    final ResponseInputStream<GetObjectResponse> stream = client.getObject(bucket.name(), object.key());
                    final Map<String, Object> map = dataStore.getObjectMap(TEST_REGION, bucket, object, url, stream, false);
                    assertEquals(url, map.get("url"));
                    assertEquals("text/plain", map.get("mimetype"));
                    assertEquals("txt", map.get("filetype"));
                    assertEquals(FILE_MAP.get(object.key()), map.get("contents"));
                    assertEquals(FilenameUtils.getName(object.key()), map.get("filename"));
                    assertEquals(dataStore.getManagementUrl(TEST_REGION, bucket.name(), object.key()), map.get("management_url"));
                    assertEquals(bucket.name(), map.get("bucket_name"));
                } catch (final URISyntaxException e) {
                    fail(e.getMessage());
                }
            });
        });
    }

    @Test
    public void test_getObjectContents() {
        final AmazonS3Client client = local.getAmazonS3Client();
        client.getBuckets(bucket -> {
            client.getObjects(bucket.name(), object -> {
                try {
                    final String url = dataStore.getUrl(local.getEndpoint(), TEST_REGION, bucket.name(), object.key());
                    final ResponseInputStream<GetObjectResponse> stream = client.getObject(bucket.name(), object.key());
                    final GetObjectResponse response = stream.response();
                    final String contents = dataStore.getObjectContents(stream, response.contentType(), object.key(), url, false);
                    assertEquals(FILE_MAP.get(object.key()), contents);
                } catch (final Exception e) {
                    fail(e.getMessage());
                }
            });
        });
    }

    @Test
    public void test_getUrl() throws Exception {
        assertEquals("https://fess.s3-ap-northeast-1.amazonaws.com/dir/d%20i%20r/sample.txt",
                dataStore.getUrl(null, "ap-northeast-1", "fess", "dir/d i r/sample.txt"));
        assertEquals("http://fess.localhost:4572/dir/d%20i%20r/sample.txt",
                dataStore.getUrl("http://localhost:4572", "ap-northeast-1", "fess", "dir/d i r/sample.txt"));
    }

    @Test
    public void test_getManagementUrl() throws Exception {
        assertEquals("https://s3.console.aws.amazon.com/s3/object/fess/dir/d%20i%20r/sample.txt?region=ap-northeast-1",
                dataStore.getManagementUrl("ap-northeast-1", "fess", "dir/d i r/sample.txt"));
    }

    @Test
    public void test_storeData() {
        final DataConfig dataConfig = new DataConfig();
        final Map<String, String> paramMap = local.getParams();
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldUrl(), "object.url");
        scriptMap.put(fessConfig.getIndexFieldTitle(), "object.key");
        scriptMap.put(fessConfig.getIndexFieldContent(), "object.contents");
        scriptMap.put(fessConfig.getIndexFieldMimetype(), "object.mimetype");
        scriptMap.put(fessConfig.getIndexFieldFiletype(), "object.filetype");
        scriptMap.put(fessConfig.getIndexFieldFilename(), "object.filename");
        scriptMap.put(fessConfig.getIndexFieldContentLength(), "object.size");
        scriptMap.put(fessConfig.getIndexFieldLastModified(), "object.last_modified");

        dataStore.storeData(dataConfig, new TestCallback() {
            @Override
            public void test(Map<String, String> paramMap, Map<String, Object> dataMap) {
                assertNotNull(dataMap.get(fessConfig.getIndexFieldUrl()));
                assertNotNull(dataMap.get(fessConfig.getIndexFieldTitle()));
                assertNotNull(dataMap.get(fessConfig.getIndexFieldContent()));
                assertNotNull(dataMap.get(fessConfig.getIndexFieldMimetype()));
                assertNotNull(dataMap.get(fessConfig.getIndexFieldFiletype()));
                assertNotNull(dataMap.get(fessConfig.getIndexFieldFilename()));
                assertNotNull(dataMap.get(fessConfig.getIndexFieldContentLength()));
                assertNotNull(dataMap.get(fessConfig.getIndexFieldLastModified()));
            }
        }, paramMap, scriptMap, defaultDataMap);
    }

    private static abstract class TestCallback implements IndexUpdateCallback {
        private long documentSize = 0;
        private long executeTime = 0;

        abstract void test(Map<String, String> paramMap, Map<String, Object> dataMap);

        @Override
        public void store(Map<String, String> paramMap, Map<String, Object> dataMap) {
            final long startTime = System.currentTimeMillis();
            test(paramMap, dataMap);
            executeTime += System.currentTimeMillis() - startTime;
            documentSize++;
        }

        @Override
        public long getDocumentSize() {
            return documentSize;
        }

        @Override
        public long getExecuteTime() {
            return executeTime;
        }

        @Override
        public void commit() {
        }
    }
}
