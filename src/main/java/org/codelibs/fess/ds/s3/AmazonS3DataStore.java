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
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
    protected static final String OBJECT_MIMETYPE = "mimetype";
    protected static final String OBJECT_FILETYPE = "filetype";
    protected static final String OBJECT_CONTENTS = "contents";
    protected static final String OBJECT_FILENAME = "filename";
    // - bucket(original)
    protected static final String OBJECT_BUCKET_NAME = "bucket_name";
    protected static final String OBJECT_BUCKET_CREATION_DATE = "creation_date";
    // - original
    protected static final String OBJECT_KEY = "key";
    protected static final String OBJECT_E_TAG = "e_tag";
    protected static final String OBJECT_LAST_MODIFIED = "last_modified";
    protected static final String OBJECT_OWNER_ID = "owner_id";
    protected static final String OBJECT_OWNER_DISPLAY_NAME = "owner_display_name";
    protected static final String OBJECT_SIZE = "size";
    protected static final String OBJECT_STORAGE_CLASS = "storage_class";

    protected static final String OBJECT_ACCEPT_RANGES = "accept_ranges";
    protected static final String OBJECT_CACHE_CONTROL = "cache_control";
    protected static final String OBJECT_CONTENT_DISPOSITION = "content_disposition";
    protected static final String OBJECT_CONTENT_ENCODING = "content_encoding";
    protected static final String OBJECT_CONTENT_LANGUAGE = "content_language";
    protected static final String OBJECT_CONTENT_LENGTH = "content_length";
    protected static final String OBJECT_CONTENT_RANGE = "content_range";
    protected static final String OBJECT_CONTENT_TYPE = "content_type";
    protected static final String OBJECT_DELETE_MARKER = "delete_marker";
    protected static final String OBJECT_EXPIRATION = "expiration";
    protected static final String OBJECT_EXPIRES = "expires";
    protected static final String OBJECT_MISSING_META = "missing_meta";
    protected static final String OBJECT_OBJECT_LOCK_LEGAL_HOLD_STATUS = "object_lock_legal_hold_status";
    protected static final String OBJECT_OBJECT_LOCK_MODE = "object_lock_mode";
    protected static final String OBJECT_OBJECT_LOCK_RETAIN_UNTIL_DATE = "object_lock_retain_until_date";
    protected static final String OBJECT_PARTS_COUNT = "parts_count";
    protected static final String OBJECT_REPLICATION_STATUS = "replication_status";
    protected static final String OBJECT_REQUEST_CHARGED = "request_charged";
    protected static final String OBJECT_RESTORE = "restore";
    protected static final String OBJECT_SERVER_SIDE_ENCRYPTION = "server_side_encryption";
    protected static final String OBJECT_SSE_CUSTOMER_ALGORITHM = "sse_customer_algorithm";
    protected static final String OBJECT_SSE_CUSTOMER_KEY_MD5 = "sse_customer_key_md5";
    protected static final String OBJECT_SSEKMS_KEY_ID = "ssekms_key_id";
    protected static final String OBJECT_TAG_COUNT = "tag_count";
    protected static final String OBJECT_VERSION_ID = "version_id";
    protected static final String OBJECT_WEBSITE_REDIRECT_LOCATION = "website_redirect_location";

    protected String extractorName = "tikaExtractor";

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

            final ResponseInputStream<GetObjectResponse> stream = client.getObject(bucket.name(), object.key());
            final GetObjectResponse response = stream.response();

            if (Stream.of(config.supportedMimeTypes).noneMatch(response.contentType()::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} is not an indexing target.", response.contentType());
                }
                return;
            }

            if (config.maxSize < object.size()) {
                throw new MaxLengthExceededException(
                        "The content length (" + object.size() + " byte) is over " + config.maxSize + " byte. The url is " + url);
            }

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> objectMap = new HashMap<>();

            logger.info("Crawling URL: {}", url);

            objectMap.put(OBJECT_URL, url);
            objectMap.put(OBJECT_MIMETYPE, response.contentType());
            objectMap.put(OBJECT_FILETYPE, ComponentUtil.getFileTypeHelper().get(response.contentType()));
            objectMap.put(OBJECT_CONTENTS, getObjectContents(stream, response.contentType(), object.key(), url, config.ignoreError));
            objectMap.put(OBJECT_FILENAME, FilenameUtils.getName(object.key()));

            objectMap.put(OBJECT_BUCKET_NAME, bucket.name());
            objectMap.put(OBJECT_BUCKET_CREATION_DATE, toDate(bucket.creationDate()));

            objectMap.put(OBJECT_KEY, object.key());
            objectMap.put(OBJECT_E_TAG, object.eTag());
            objectMap.put(OBJECT_LAST_MODIFIED, toDate(object.lastModified()));
            final Owner owner = object.owner();
            objectMap.put(OBJECT_OWNER_ID, Objects.nonNull(owner) ? owner.id() : null);
            objectMap.put(OBJECT_OWNER_DISPLAY_NAME, Objects.nonNull(owner) ? owner.displayName() : null);
            objectMap.put(OBJECT_SIZE, object.size());
            objectMap.put(OBJECT_STORAGE_CLASS, object.storageClassAsString());
            objectMap.put(OBJECT_ACCEPT_RANGES, response.acceptRanges());
            objectMap.put(OBJECT_CACHE_CONTROL, response.cacheControl());
            objectMap.put(OBJECT_CONTENT_DISPOSITION, response.contentDisposition());
            objectMap.put(OBJECT_CONTENT_ENCODING, response.contentEncoding());
            objectMap.put(OBJECT_CONTENT_LANGUAGE, response.contentLanguage());
            objectMap.put(OBJECT_CONTENT_LENGTH, response.contentLength());
            objectMap.put(OBJECT_CONTENT_RANGE, response.contentRange());
            objectMap.put(OBJECT_CONTENT_TYPE, response.contentType());
            objectMap.put(OBJECT_DELETE_MARKER, response.deleteMarker());
            objectMap.put(OBJECT_EXPIRATION, response.expiration());
            objectMap.put(OBJECT_EXPIRES, toDate(response.expires()));
            objectMap.put(OBJECT_MISSING_META, response.missingMeta());
            objectMap.put(OBJECT_OBJECT_LOCK_LEGAL_HOLD_STATUS, response.objectLockLegalHoldStatusAsString());
            objectMap.put(OBJECT_OBJECT_LOCK_MODE, response.objectLockModeAsString());
            objectMap.put(OBJECT_OBJECT_LOCK_RETAIN_UNTIL_DATE, toDate(response.objectLockRetainUntilDate()));
            objectMap.put(OBJECT_PARTS_COUNT, response.partsCount());
            objectMap.put(OBJECT_REPLICATION_STATUS, response.replicationStatusAsString());
            objectMap.put(OBJECT_REQUEST_CHARGED, response.requestChargedAsString());
            objectMap.put(OBJECT_RESTORE, response.restore());
            objectMap.put(OBJECT_SERVER_SIDE_ENCRYPTION, response.serverSideEncryptionAsString());
            objectMap.put(OBJECT_SSE_CUSTOMER_ALGORITHM, response.sseCustomerAlgorithm());
            objectMap.put(OBJECT_SSE_CUSTOMER_KEY_MD5, response.sseCustomerKeyMD5());
            objectMap.put(OBJECT_SSEKMS_KEY_ID, response.ssekmsKeyId());
            objectMap.put(OBJECT_TAG_COUNT, response.tagCount());
            objectMap.put(OBJECT_VERSION_ID, response.versionId());
            objectMap.put(OBJECT_WEBSITE_REDIRECT_LOCATION, response.websiteRedirectLocation());

            resultMap.put(OBJECT, objectMap);

            if (logger.isDebugEnabled()) {
                logger.debug("objectMap: {}", objectMap);
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

    protected String getObjectContents(final ResponseInputStream<GetObjectResponse> in, final String contentType, final String key,
            final String url, final boolean ignoreError) {
        try {
            Extractor extractor = ComponentUtil.getExtractorFactory().getExtractor(contentType);
            if (extractor == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("use a default extractor as {} by {}", extractorName, contentType);
                }
                extractor = ComponentUtil.getComponent(extractorName);
            }
            return extractor.getText(in, null).getContent();
        } catch (final Exception e) {
            if (ignoreError) {
                logger.warn("Failed to get contents: " + key, e);
                return StringUtil.EMPTY;
            } else {
                throw new DataStoreCrawlingException(url, "Failed to get contents: " + key, e);
            }
        }
    }

    protected String getUrl(final String region, final String bucket, final String object) throws URISyntaxException {
        return new URI("https", bucket + ".s3-" + region + ".amazonaws.com", "/" + object, null).toASCIIString();
    }

    protected Date toDate(final Instant instant) {
        return Objects.nonNull(instant) ? Date.from(instant) : null;
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
