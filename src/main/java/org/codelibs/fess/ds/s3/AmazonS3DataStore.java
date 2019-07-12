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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AmazonS3DataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(AmazonS3DataStore.class);

    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    // parameters
    protected static final String MAX_SIZE = "max_size";
    protected static final String IGNORE_ERROR = "ignore_error";
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";

    // scripts
    protected static final String OBJECT = "object";
    // - custom
    protected static final String OBJECT_URL = "url";
    // TODO add scripts
    // - bucket(original)
    protected static final String OBJECT_BUCKET_NAME = "bucket_name";
    protected static final String OBJECT_BUCKET_CREATION_DATE = "creation_date";
    // - original
    protected static final String OBJECT_KEY = "key";
    protected static final String OBJECT_E_TAG = "e_tag";
    protected static final String OBJECT_LAST_MODIFIED = "last_modified";

    protected String getName() {
        return "AmazonS3";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Config config = new Config(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("config: {}", config);
        }
        final ExecutorService executorService =
                Executors.newFixedThreadPool(Integer.parseInt(paramMap.getOrDefault(NUMBER_OF_THREADS, "1")));

        try {
            final AmazonS3Client client = createClient(paramMap);
            crawlBuckets(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, executorService, client);
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Interrupted.", e);
            }
        } finally {
            executorService.shutdown();
        }
    }

    protected void crawlBuckets(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config,
            final ExecutorService executorService, final AmazonS3Client client) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling buckets.");
        }
        client.getBuckets(bucket -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling bucket objects: {}", bucket.name());
            }
            client.getObjects(bucket.name(), object -> executorService
                    .execute(() -> storeObject(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, client, bucket, object)));
        });
    }

    protected void storeObject(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config, final AmazonS3Client client,
            final Bucket bucket, final S3Object object) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            final String url = getUrl(client.getRegion().id(), bucket.name(), object.key());

            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                return;
            }

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> objectMap = new HashMap<>();

            logger.info("Crawling URL: {}", url);

            objectMap.put(OBJECT_URL, url);
            // TODO add scripts

            objectMap.put(OBJECT_BUCKET_NAME, bucket.name());
            objectMap.put(OBJECT_BUCKET_CREATION_DATE, Date.from(bucket.creationDate()));

            objectMap.put(OBJECT_KEY, object.key());
            objectMap.put(OBJECT_E_TAG, object.eTag());
            objectMap.put(OBJECT_LAST_MODIFIED, Date.from(object.lastModified()));

            resultMap.put(OBJECT, objectMap);

            if (logger.isDebugEnabled()) {
                logger.debug("fileMap: {}", objectMap);
            }

            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);

            Throwable target = e;
            if (target instanceof MultipleCrawlingAccessException) {
                final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, "", target);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : " + dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), "", t);
        }
    }

    protected String getUrl(final String region, final String bucket, final String object) throws URISyntaxException {
        return new URI("https", bucket + ".s3-" + region + ".amazonaws.com", "/" + object, null).toASCIIString();
    }

    protected AmazonS3Client createClient(final Map<String, String> paramMap) {
        return new AmazonS3Client(paramMap);
    }

    protected static class Config {
        final long maxSize;
        final boolean ignoreError;
        final String[] supportedMimeTypes;
        final UrlFilter urlFilter;

        Config(final Map<String, String> paramMap) {
            maxSize = getMaxSize(paramMap);
            ignoreError = isIgnoreError(paramMap);
            supportedMimeTypes = getSupportedMimeTypes(paramMap);
            urlFilter = getUrlFilter(paramMap);
        }

        private long getMaxSize(final Map<String, String> paramMap) {
            final String value = paramMap.get(MAX_SIZE);
            try {
                return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
            } catch (final NumberFormatException e) {
                return DEFAULT_MAX_SIZE;
            }
        }

        private boolean isIgnoreError(final Map<String, String> paramMap) {
            return paramMap.getOrDefault(IGNORE_ERROR, Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
        }

        private String[] getSupportedMimeTypes(final Map<String, String> paramMap) {
            return StreamUtil.split(paramMap.getOrDefault(SUPPORTED_MIMETYPES, ".*"), ",")
                    .get(stream -> stream.map(String::trim).toArray(String[]::new));
        }

        private UrlFilter getUrlFilter(final Map<String, String> paramMap) {
            final UrlFilter urlFilter;
            try {
                urlFilter = ComponentUtil.getComponent(UrlFilter.class);
            } catch (final ComponentNotFoundException e) {
                return null;
            }
            final String include = paramMap.get(INCLUDE_PATTERN);
            if (StringUtil.isNotBlank(include)) {
                urlFilter.addInclude(include);
            }
            final String exclude = paramMap.get(EXCLUDE_PATTERN);
            if (StringUtil.isNotBlank(exclude)) {
                urlFilter.addExclude(exclude);
            }
            urlFilter.init(paramMap.get(Constants.CRAWLING_INFO_ID));
            if (logger.isDebugEnabled()) {
                logger.debug("urlFilter: {}", urlFilter);
            }
            return urlFilter;
        }

        @Override
        public String toString() {
            return "{maxSize=" + maxSize + ",ignoreError=" + ignoreError + ",supportedMimeTypes=" + Arrays.toString(supportedMimeTypes)
                    + ",urlFilter=" + urlFilter + "}";
        }
    }

}
