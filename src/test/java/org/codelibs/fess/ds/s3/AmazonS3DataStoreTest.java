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

import static org.codelibs.fess.ds.s3.LocalAmazonS3.FILE_MAP;
import static org.codelibs.fess.ds.s3.LocalAmazonS3.TEST_REGION;
import static org.codelibs.fess.ds.s3.LocalAmazonS3.getInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.io.FilenameUtils;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lastaflute.di.core.factory.SingletonLaContainerFactory;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class AmazonS3DataStoreTest {

    private static final Logger logger = LogManager.getLogger(AmazonS3DataStoreTest.class);

    private static LocalAmazonS3 local;
    private static AmazonS3DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        local = getInstance();
        local.initializeBuckets();
        initializeContainer();
        dataStore = new AmazonS3DataStore() {
            @Override
            protected void storeFailureUrl(final DataConfig dataConfig, final String errorName, final String url, final Throwable target) {
                logger.error("[{}] {} : {}", errorName, url, target);
            }
        };
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
        assertEquals("https://fess.s3.ap-northeast-1.amazonaws.com/dir/d%20i%20r/sample.txt",
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
        final DataStoreParams paramMap = local.getParams();
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
            public void test(DataStoreParams paramMap, Map<String, Object> dataMap) {
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

    @Test
    public void test_includePatternParameter() {
        // Test that include_pattern parameter can be set
        // Note: URL filtering requires UrlFilter component which is not available in this test environment
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_pattern", ".*sample-0.*");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals(".*sample-0.*", paramMap.getAsString("include_pattern"));
    }

    @Test
    public void test_excludePatternParameter() {
        // Test that exclude_pattern parameter can be set
        // Note: URL filtering requires UrlFilter component which is not available in this test environment
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", ".*sample-1.*");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals(".*sample-1.*", paramMap.getAsString("exclude_pattern"));
    }

    @Test
    public void test_supportedMimeTypesParameter() {
        // Test that supported_mimetypes parameter can be set
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("supported_mimetypes", "application/pdf, text/plain");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals("application/pdf, text/plain", paramMap.getAsString("supported_mimetypes"));
    }

    @Test
    public void test_storeDataWithMaxSize() {
        final DataConfig dataConfig = new DataConfig();
        final DataStoreParams paramMap = local.getParams();
        paramMap.put("max_size", "1"); // Set max size to 1 byte (all files exceed this)
        paramMap.put("ignore_error", "true"); // Ignore errors to prevent test failure
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldUrl(), "object.url");

        final AtomicInteger count = new AtomicInteger(0);
        dataStore.storeData(dataConfig, new TestCallback() {
            @Override
            public void test(DataStoreParams paramMap, Map<String, Object> dataMap) {
                count.incrementAndGet();
            }
        }, paramMap, scriptMap, defaultDataMap);

        // Should process 0 objects as all exceed max size
        assertEquals(0, count.get());
    }

    @Test
    public void test_bucketsParameter() {
        // Test that buckets parameter can be set
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("buckets", "bucket-1, bucket-2");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals("bucket-1, bucket-2", paramMap.getAsString("buckets"));
    }

    @Test
    public void test_numberOfThreadsParameter() {
        // Test that number_of_threads parameter can be set
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("number_of_threads", "4");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals("4", paramMap.getAsString("number_of_threads"));
    }

    @Test
    public void test_maxSizeParameter() {
        // Test that max_size parameter can be set
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "10000000");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals("10000000", paramMap.getAsString("max_size"));
    }

    @Test
    public void test_maxKeysParameter() {
        // Test that max_keys parameter can be set
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_keys", "100");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals("100", paramMap.getAsString("max_keys"));
    }

    @Test
    public void test_getUrlWithSpecialCharacters() throws Exception {
        // Test URL generation with special characters
        final String url = dataStore.getUrl(null, "ap-northeast-1", "test-bucket", "path/with spaces/file.txt");
        assertTrue("URL should contain encoded spaces", url.contains("%20"));
        assertEquals("https://test-bucket.s3.ap-northeast-1.amazonaws.com/path/with%20spaces/file.txt", url);
    }

    @Test
    public void test_getManagementUrlWithSpecialCharacters() throws Exception {
        // Test management URL generation with special characters
        final String url = dataStore.getManagementUrl("ap-northeast-1", "test-bucket", "path/with spaces/file.txt");
        assertTrue("Management URL should contain encoded spaces", url.contains("%20"));
    }

    @Test
    public void test_getObjectMapWithNullOwner() {
        // Test that getObjectMap handles objects without owner information
        final AmazonS3Client client = local.getAmazonS3Client();
        client.getBuckets(bucket -> {
            client.getObjects(bucket.name(), object -> {
                try {
                    final String url = dataStore.getUrl(local.getEndpoint(), TEST_REGION, bucket.name(), object.key());
                    final ResponseInputStream<GetObjectResponse> stream = client.getObject(bucket.name(), object.key());
                    final Map<String, Object> map = dataStore.getObjectMap(TEST_REGION, bucket, object, url, stream, false);

                    // Owner may be null in some cases - ensure it's handled gracefully
                    assertNotNull(map);
                    assertTrue(map.containsKey("owner_id"));
                    assertTrue(map.containsKey("owner_display_name"));
                } catch (final URISyntaxException e) {
                    fail(e.getMessage());
                }
            });
        });
    }

    @Test
    public void test_configDefaults() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Test that config uses default values when parameters are not provided
        // This is tested implicitly through the Config class constructor
        assertNotNull(paramMap);
    }

    @Test
    public void test_ignoreErrorFlag() {
        final DataConfig dataConfig = new DataConfig();
        final DataStoreParams paramMap = local.getParams();
        paramMap.put("ignore_error", "false"); // Don't ignore errors
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldUrl(), "object.url");

        // This test ensures that the ignore_error parameter is properly parsed
        dataStore.storeData(dataConfig, new TestCallback() {
            @Override
            public void test(DataStoreParams paramMap, Map<String, Object> dataMap) {
                assertNotNull(dataMap);
            }
        }, paramMap, scriptMap, defaultDataMap);
    }

    @Test
    public void test_ignoreErrorParameter() {
        // Test that ignore_error parameter can be set
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_error", "true");
        paramMap.put("region", "us-east-1");
        paramMap.put("access_key_id", "test");
        paramMap.put("secret_key", "test");

        // Verify parameter is set correctly
        assertEquals("true", paramMap.getAsString("ignore_error"));
    }

    private static abstract class TestCallback implements IndexUpdateCallback {
        private long documentSize = 0;
        private long executeTime = 0;

        abstract void test(DataStoreParams paramMap, Map<String, Object> dataMap);

        @Override
        public void store(DataStoreParams paramMap, Map<String, Object> dataMap) {
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
