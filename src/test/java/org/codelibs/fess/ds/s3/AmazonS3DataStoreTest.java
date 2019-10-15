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
import org.codelibs.fess.crawler.container.StandardCrawlerContainer;
import org.codelibs.fess.crawler.extractor.ExtractorFactory;
import org.codelibs.fess.crawler.extractor.impl.TextExtractor;
import org.codelibs.fess.helper.FileTypeHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lastaflute.di.core.factory.SingletonLaContainerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static cloud.localstack.Localstack.getEndpointS3;
import static cloud.localstack.TestUtils.DEFAULT_REGION;
import static org.codelibs.fess.ds.s3.TestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(LocalstackTestRunner.class)
public class AmazonS3DataStoreTest {

    private static AmazonS3DataStore dataStore;

    @BeforeClass
    public static void setUp() throws Exception {
        initializeContainer();
        initializeComponents();
        initializeBuckets();
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

    private static void initializeComponents() {
        final StandardCrawlerContainer crawlerContainer = new StandardCrawlerContainer() //
                .singleton("fileTypeHelper", FileTypeHelper.class) //
                .singleton("textExtractor", TextExtractor.class) //
                .singleton("extractorFactory", ExtractorFactory.class);
        final ExtractorFactory extractorFactory = crawlerContainer.getComponent("extractorFactory");
        extractorFactory.addExtractor("text/plain", crawlerContainer.getComponent("textExtractor"));
        ComponentUtil.register(crawlerContainer.getComponent("fileTypeHelper"), "fileTypeHelper");
        ComponentUtil.register(crawlerContainer.getComponent("textExtractor"), "textExtractor");
        ComponentUtil.register(extractorFactory, "extractorFactory");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        resetBuckets();
        ComponentUtil.setFessConfig(null);
    }

    @Test
    public void test_getObjectMap() {
        final AmazonS3Client client = getClient();
        client.getBuckets(bucket -> {
            if (Objects.equals(bucket.name(), BUCKET_NAME)) {
                client.getObjects(bucket.name(), object -> {
                    try {
                        final String url = dataStore.getUrl(getEndpointS3(), DEFAULT_REGION, bucket.name(), object.key());
                        final ResponseInputStream<GetObjectResponse> stream = client.getObject(bucket.name(), object.key());
                        final Map<String, Object> map = dataStore.getObjectMap(DEFAULT_REGION, bucket, object, url, stream, false);
                        if (Objects.equals(object.key(), FILES[0])) {
                            assertEquals("hogehoge", map.get("contents"));
                        } else if (Objects.equals(object.key(), FILES[1])) {
                            assertEquals("hugahuga", map.get("contents"));
                        }
                    } catch (final URISyntaxException e) {
                        fail(e.getMessage());
                    }
                });
            }
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

}
