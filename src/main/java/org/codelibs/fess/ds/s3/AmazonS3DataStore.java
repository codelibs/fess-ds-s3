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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.io.FilenameUtils;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.io.CopyUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.crawler.helper.MimeTypeHelper;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * A data store for crawling and indexing objects stored in Amazon S3 (or S3-compatible storage).
 *
 * <p>
 * This data store lists buckets and their objects, extracts their contents, and stores them via the
 * {@link IndexUpdateCallback}. Crawling behavior is controlled by the configured {@link DataStoreParams}.
 * </p>
 */
public class AmazonS3DataStore extends AbstractDataStore {

    private static final Logger logger = LogManager.getLogger(AmazonS3DataStore.class);

    /**
     * Creates a new instance.
     */
    public AmazonS3DataStore() {
        // do nothing
    }

    /** The default maximum number of keys to fetch per request. */
    protected static final int DEFAULT_MAX_KEYS = 1000;

    /** The default maximum object size in bytes (10 MB). */
    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    // parameters
    /** The parameter name for the maximum number of keys per request. */
    protected static final String MAX_KEYS = "max_keys";

    /** The parameter name for the maximum object size. */
    protected static final String MAX_SIZE = "max_size";

    /** The parameter name for whether to ignore errors. */
    protected static final String IGNORE_ERROR = "ignore_error";

    /** The parameter name for the supported MIME types. */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";

    /** The parameter name for the URL include pattern. */
    protected static final String INCLUDE_PATTERN = "include_pattern";

    /** The parameter name for the URL exclude pattern. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";

    /** The parameter name for the number of crawler threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";

    /** The parameter name for the target bucket names. */
    protected static final String BUCKETS = "buckets";

    // scripts
    /** The script key for the object map. */
    protected static final String OBJECT = "object";
    // - custom
    /** The script key for the object URL. */
    protected static final String OBJECT_URL = "url";

    /** The script key for the object MIME type. */
    protected static final String OBJECT_MIMETYPE = "mimetype";

    /** The script key for the object file type. */
    protected static final String OBJECT_FILETYPE = "filetype";

    /** The script key for the extracted object contents. */
    protected static final String OBJECT_CONTENTS = "contents";

    /** The script key for the object file name. */
    protected static final String OBJECT_FILENAME = "filename";

    /** The script key for the management console URL. */
    protected static final String OBJECT_MANAGEMENT_URL = "management_url";
    // - bucket(original)
    /** The script key for the bucket name. */
    protected static final String OBJECT_BUCKET_NAME = "bucket_name";

    /** The script key for the bucket creation date. */
    protected static final String OBJECT_BUCKET_CREATION_DATE = "creation_date";
    // - original
    /** The script key for the object key. */
    protected static final String OBJECT_KEY = "key";

    /** The script key for the object ETag. */
    protected static final String OBJECT_E_TAG = "e_tag";

    /** The script key for the object last modified date. */
    protected static final String OBJECT_LAST_MODIFIED = "last_modified";

    /** The script key for the object owner ID. */
    protected static final String OBJECT_OWNER_ID = "owner_id";

    /** The script key for the object owner display name. */
    protected static final String OBJECT_OWNER_DISPLAY_NAME = "owner_display_name";

    /** The script key for the object size. */
    protected static final String OBJECT_SIZE = "size";

    /** The script key for the object storage class. */
    protected static final String OBJECT_STORAGE_CLASS = "storage_class";

    /** The script key for the accept-ranges header. */
    protected static final String OBJECT_ACCEPT_RANGES = "accept_ranges";

    /** The script key for the cache-control header. */
    protected static final String OBJECT_CACHE_CONTROL = "cache_control";

    /** The script key for the content-disposition header. */
    protected static final String OBJECT_CONTENT_DISPOSITION = "content_disposition";

    /** The script key for the content-encoding header. */
    protected static final String OBJECT_CONTENT_ENCODING = "content_encoding";

    /** The script key for the content-language header. */
    protected static final String OBJECT_CONTENT_LANGUAGE = "content_language";

    /** The script key for the content-length header. */
    protected static final String OBJECT_CONTENT_LENGTH = "content_length";

    /** The script key for the content-range header. */
    protected static final String OBJECT_CONTENT_RANGE = "content_range";

    /** The script key for the content-type header. */
    protected static final String OBJECT_CONTENT_TYPE = "content_type";

    /** The script key for the delete-marker flag. */
    protected static final String OBJECT_DELETE_MARKER = "delete_marker";

    /** The script key for the object expiration. */
    protected static final String OBJECT_EXPIRATION = "expiration";

    /** The script key for the object expires value. */
    protected static final String OBJECT_EXPIRES = "expires";

    /** The script key for the missing-meta count. */
    protected static final String OBJECT_MISSING_META = "missing_meta";

    /** The script key for the object lock legal hold status. */
    protected static final String OBJECT_OBJECT_LOCK_LEGAL_HOLD_STATUS = "object_lock_legal_hold_status";

    /** The script key for the object lock mode. */
    protected static final String OBJECT_OBJECT_LOCK_MODE = "object_lock_mode";

    /** The script key for the object lock retain-until date. */
    protected static final String OBJECT_OBJECT_LOCK_RETAIN_UNTIL_DATE = "object_lock_retain_until_date";

    /** The script key for the parts count. */
    protected static final String OBJECT_PARTS_COUNT = "parts_count";

    /** The script key for the replication status. */
    protected static final String OBJECT_REPLICATION_STATUS = "replication_status";

    /** The script key for the request-charged value. */
    protected static final String OBJECT_REQUEST_CHARGED = "request_charged";

    /** The script key for the restore status. */
    protected static final String OBJECT_RESTORE = "restore";

    /** The script key for the server-side encryption. */
    protected static final String OBJECT_SERVER_SIDE_ENCRYPTION = "server_side_encryption";

    /** The script key for the SSE customer algorithm. */
    protected static final String OBJECT_SSE_CUSTOMER_ALGORITHM = "sse_customer_algorithm";

    /** The script key for the SSE customer key MD5. */
    protected static final String OBJECT_SSE_CUSTOMER_KEY_MD5 = "sse_customer_key_md5";

    /** The script key for the SSE KMS key ID. */
    protected static final String OBJECT_SSEKMS_KEY_ID = "ssekms_key_id";

    /** The script key for the tag count. */
    protected static final String OBJECT_TAG_COUNT = "tag_count";

    /** The script key for the version ID. */
    protected static final String OBJECT_VERSION_ID = "version_id";

    /** The script key for the website redirect location. */
    protected static final String OBJECT_WEBSITE_REDIRECT_LOCATION = "website_redirect_location";

    /** The name of the extractor used to extract object contents. */
    protected String extractorName = "tikaExtractor";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Config config = new Config(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("config: {}", config);
        }
        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));

        try (final AmazonS3Client client = createClient(paramMap)) {
            crawlBuckets(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, executorService, client);
            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
            }
            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Crawls the target buckets and submits each object to be stored.
     *
     * @param dataConfig the data configuration
     * @param callback the callback for updating the index
     * @param paramMap the data store parameters
     * @param scriptMap the script map for converting values
     * @param defaultDataMap the default data map
     * @param config the crawling configuration
     * @param executorService the executor service for processing objects
     * @param client the S3 client
     */
    protected void crawlBuckets(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config,
            final ExecutorService executorService, final AmazonS3Client client) {
        final Consumer<Bucket> processOnBucket = bucket -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling bucket objects: {}", bucket.name());
            }
            client.getObjects(bucket.name(), config.maxKeys, object -> executorService
                    .execute(() -> storeObject(dataConfig, callback, paramMap, scriptMap, defaultDataMap, config, client, bucket, object)));
        };
        final String bucketNames = paramMap.getAsString(BUCKETS);
        if (StringUtil.isNotBlank(bucketNames)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling {} buckets.", bucketNames);
            }
            client.getBuckets(StreamUtil.split(bucketNames, ",").get(stream -> stream.map(s -> s.trim()).toArray(n -> new String[n])),
                    processOnBucket);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling all buckets.");
            }
            client.getBuckets(processOnBucket);
        }
    }

    /**
     * Extracts and stores a single S3 object.
     *
     * @param dataConfig the data configuration
     * @param callback the callback for updating the index
     * @param paramMap the data store parameters
     * @param scriptMap the script map for converting values
     * @param defaultDataMap the default data map
     * @param config the crawling configuration
     * @param client the S3 client
     * @param bucket the bucket containing the object
     * @param object the object to store
     */
    protected void storeObject(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Config config, final AmazonS3Client client,
            final Bucket bucket, final S3Object object) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final StatsKeyObject statsKey = new StatsKeyObject(bucket.name() + "@" + object.key());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        String url = StringUtil.EMPTY;
        try {
            crawlerStatsHelper.begin(statsKey);
            url = getUrl(client.getEndpoint(), client.getRegion().id(), bucket.name(), object.key());

            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final ResponseInputStream<GetObjectResponse> stream = client.getObject(bucket.name(), object.key());
            final GetObjectResponse response = stream.response();

            if (Stream.of(config.supportedMimeTypes).noneMatch(response.contentType()::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} is not an indexing target.", response.contentType());
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            if (config.maxSize < object.size()) {
                throw new MaxLengthExceededException(
                        "The content length (" + object.size() + " byte) is over " + config.maxSize + " byte. The url is " + url);
            }

            logger.info("Crawling URL: {}", url);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> objectMap = getObjectMap(client.getRegion().id(), bucket, object, url, stream, config.ignoreError);
            resultMap.put(OBJECT, objectMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("objectMap: {}", objectMap);
            }

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            if (dataMap.get("url") instanceof final String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

            Throwable target = e;
            if (target instanceof final MultipleCrawlingAccessException ex) {
                final Throwable[] causes = ex.getCauses();
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

            storeFailureUrl(dataConfig, errorName, url, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            storeFailureUrl(dataConfig, t.getClass().getCanonicalName(), url, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Stores a failure URL when crawling an object fails.
     *
     * @param dataConfig the data configuration
     * @param errorName the name of the error
     * @param url the URL that failed
     * @param target the throwable that caused the failure
     */
    protected void storeFailureUrl(final DataConfig dataConfig, final String errorName, final String url, final Throwable target) {
        final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
        failureUrlService.store(dataConfig, errorName, url, target);
    }

    /**
     * Builds a map of metadata and extracted contents for the given S3 object.
     *
     * @param region the AWS region
     * @param bucket the bucket containing the object
     * @param object the S3 object
     * @param url the URL of the object
     * @param stream the response input stream for the object
     * @param ignoreError whether to ignore content extraction errors
     * @return a map of object metadata and contents
     * @throws URISyntaxException if a URL cannot be constructed
     */
    protected Map<String, Object> getObjectMap(final String region, final Bucket bucket, final S3Object object, final String url,
            final ResponseInputStream<GetObjectResponse> stream, final boolean ignoreError) throws URISyntaxException {
        final Map<String, Object> map = new HashMap<>();
        final GetObjectResponse response = stream.response();
        map.put(OBJECT_URL, url);
        final String filename = FilenameUtils.getName(object.key());
        map.put(OBJECT_FILENAME, filename);
        map.put(OBJECT_MANAGEMENT_URL, getManagementUrl(region, bucket.name(), object.key()));

        map.put(OBJECT_BUCKET_NAME, bucket.name());
        map.put(OBJECT_BUCKET_CREATION_DATE, toDate(bucket.creationDate()));

        map.put(OBJECT_KEY, object.key());
        map.put(OBJECT_E_TAG, object.eTag());
        map.put(OBJECT_LAST_MODIFIED, toDate(object.lastModified()));
        final Owner owner = object.owner();
        map.put(OBJECT_OWNER_ID, Objects.nonNull(owner) ? owner.id() : null);
        map.put(OBJECT_OWNER_DISPLAY_NAME, Objects.nonNull(owner) ? owner.displayName() : null);
        map.put(OBJECT_SIZE, object.size());
        map.put(OBJECT_STORAGE_CLASS, object.storageClassAsString());
        map.put(OBJECT_ACCEPT_RANGES, response.acceptRanges());
        map.put(OBJECT_CACHE_CONTROL, response.cacheControl());
        map.put(OBJECT_CONTENT_DISPOSITION, response.contentDisposition());
        map.put(OBJECT_CONTENT_ENCODING, response.contentEncoding());
        map.put(OBJECT_CONTENT_LANGUAGE, response.contentLanguage());
        map.put(OBJECT_CONTENT_LENGTH, response.contentLength());
        map.put(OBJECT_CONTENT_RANGE, response.contentRange());
        map.put(OBJECT_DELETE_MARKER, response.deleteMarker());
        map.put(OBJECT_EXPIRATION, response.expiration());
        map.put(OBJECT_EXPIRES, toDate(response.expires()));
        map.put(OBJECT_MISSING_META, response.missingMeta());
        map.put(OBJECT_OBJECT_LOCK_LEGAL_HOLD_STATUS, response.objectLockLegalHoldStatusAsString());
        map.put(OBJECT_OBJECT_LOCK_MODE, response.objectLockModeAsString());
        map.put(OBJECT_OBJECT_LOCK_RETAIN_UNTIL_DATE, toDate(response.objectLockRetainUntilDate()));
        map.put(OBJECT_PARTS_COUNT, response.partsCount());
        map.put(OBJECT_REPLICATION_STATUS, response.replicationStatusAsString());
        map.put(OBJECT_REQUEST_CHARGED, response.requestChargedAsString());
        map.put(OBJECT_RESTORE, response.restore());
        map.put(OBJECT_SERVER_SIDE_ENCRYPTION, response.serverSideEncryptionAsString());
        map.put(OBJECT_SSE_CUSTOMER_ALGORITHM, response.sseCustomerAlgorithm());
        map.put(OBJECT_SSE_CUSTOMER_KEY_MD5, response.sseCustomerKeyMD5());
        map.put(OBJECT_SSEKMS_KEY_ID, response.ssekmsKeyId());
        map.put(OBJECT_TAG_COUNT, response.tagCount());
        map.put(OBJECT_VERSION_ID, response.versionId());
        map.put(OBJECT_WEBSITE_REDIRECT_LOCATION, response.websiteRedirectLocation());
        String contentType = response.contentType();
        DeferredFileOutputStream dfos = null;
        try (DeferredFileOutputStream out = new DeferredFileOutputStream(1000000, "fess-ds-s3-", ".out", null)) {
            dfos = out;
            CopyUtil.copy(stream, out);
            out.flush();
            contentType = getMimeType(filename, out);
            try (InputStream is = getContentInputStream(out)) {
                map.put(OBJECT_CONTENTS, getObjectContents(is, contentType, object.key(), url, ignoreError));
            }
        } catch (final IOException e) {
            logger.warn("Failed to process {}", url, e);
        } finally {
            if (dfos != null && !dfos.isInMemory()) {
                final File file = dfos.getFile();
                if (!file.delete()) {
                    logger.warn("Failed to delete {}.", file.getAbsolutePath());
                }
            }
        }
        map.put(OBJECT_FILETYPE, ComponentUtil.getFileTypeHelper().get(contentType));
        map.put(OBJECT_MIMETYPE, contentType);
        map.put(OBJECT_CONTENT_TYPE, contentType);
        return map;
    }

    /**
     * Determines the MIME type of the given content.
     *
     * @param filename the file name used as a hint
     * @param out the buffered content
     * @return the detected MIME type
     * @throws IOException if the content cannot be read
     */
    protected String getMimeType(final String filename, final DeferredFileOutputStream out) throws IOException {
        final MimeTypeHelper mimeTypeHelper = ComponentUtil.getComponent(MimeTypeHelper.class);
        try (InputStream is = getContentInputStream(out)) {
            return mimeTypeHelper.getContentType(is, filename);
        }
    }

    /**
     * Returns an input stream for reading the given buffered content.
     *
     * @param out the buffered content
     * @return an input stream over the content, either in-memory or file-backed
     * @throws IOException if the content cannot be read
     */
    protected InputStream getContentInputStream(final DeferredFileOutputStream out) throws IOException {
        if (out.isInMemory()) {
            return new ByteArrayInputStream(out.getData());
        }
        return new FileInputStream(out.getFile());
    }

    /**
     * Extracts text contents from the given input stream.
     *
     * @param in the input stream of the object contents
     * @param contentType the MIME type of the contents
     * @param key the key of the object
     * @param url the URL of the object
     * @param ignoreError whether to ignore extraction errors
     * @return the extracted text contents, or an empty string if extraction fails and errors are ignored
     */
    protected String getObjectContents(final InputStream in, final String contentType, final String key, final String url,
            final boolean ignoreError) {
        try {
            return ComponentUtil.getExtractorFactory()
                    .builder(in, null)
                    .mimeType(contentType)
                    .extractorName(extractorName)
                    .extract()
                    .getContent();
        } catch (final Exception e) {
            if (!ignoreError && !ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new DataStoreCrawlingException(url, "Failed to get contents: " + key, e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Failed to get contents: {}", key, e);
            } else {
                logger.warn("Failed to get contents: {}. {}", key, e.getMessage());
            }
            return StringUtil.EMPTY;
        }
    }

    /**
     * Builds the URL for the given object.
     *
     * @param endpoint the S3 endpoint override, or {@code null} for AWS
     * @param region the AWS region
     * @param bucket the name of the bucket
     * @param object the key of the object
     * @return the object URL
     * @throws URISyntaxException if the URL cannot be constructed
     */
    protected String getUrl(final String endpoint, final String region, final String bucket, final String object)
            throws URISyntaxException {
        if (Objects.nonNull(endpoint)) {
            final URI uri = URI.create(endpoint);
            return new URI(uri.getScheme(), bucket + "." + uri.getAuthority(), "/" + object, null, null).toASCIIString();
        }
        // Virtual Hosted-Stype: https://my-bucket.s3.us-west-2.amazonaws.com/puppy.png
        return new URI("https", bucket + ".s3." + region + ".amazonaws.com", "/" + object, null).toASCIIString();
    }

    /**
     * Builds the AWS management console URL for the given object.
     *
     * @param region the AWS region
     * @param bucket the name of the bucket
     * @param object the key of the object
     * @return the management console URL
     * @throws URISyntaxException if the URL cannot be constructed
     */
    protected String getManagementUrl(final String region, final String bucket, final String object) throws URISyntaxException {
        return new URI("https", "s3.console.aws.amazon.com", "/s3/object/" + bucket + "/" + object, "region=" + region, null)
                .toASCIIString();
    }

    /**
     * Converts the given instant to a {@link Date}.
     *
     * @param instant the instant to convert, may be {@code null}
     * @return the corresponding date, or {@code null} if the instant is {@code null}
     */
    protected Date toDate(final Instant instant) {
        return Objects.nonNull(instant) ? Date.from(instant) : null;
    }

    /**
     * Creates a fixed-size thread pool for processing objects.
     *
     * @param nThreads the number of threads
     * @return a new executor service
     */
    protected ExecutorService newFixedThreadPool(final int nThreads) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executor Thread Pool: {}", nThreads);
        }
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * Creates an S3 client from the given parameters.
     *
     * @param paramMap the data store parameters
     * @return a new S3 client
     */
    protected AmazonS3Client createClient(final DataStoreParams paramMap) {
        return new AmazonS3Client(paramMap);
    }

    /**
     * Holds the crawling configuration derived from the data store parameters.
     */
    protected static class Config {
        final int maxKeys;
        final long maxSize;
        final boolean ignoreError;
        final String[] supportedMimeTypes;
        final UrlFilter urlFilter;

        Config(final DataStoreParams paramMap) {
            maxKeys = getMaxKeys(paramMap);
            maxSize = getMaxSize(paramMap);
            ignoreError = isIgnoreError(paramMap);
            supportedMimeTypes = getSupportedMimeTypes(paramMap);
            urlFilter = getUrlFilter(paramMap);
        }

        private int getMaxKeys(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(MAX_KEYS);
            try {
                return StringUtil.isNotBlank(value) ? Integer.parseInt(value) : DEFAULT_MAX_KEYS;
            } catch (final NumberFormatException e) {
                return DEFAULT_MAX_KEYS;
            }
        }

        private long getMaxSize(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(MAX_SIZE);
            try {
                return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
            } catch (final NumberFormatException e) {
                return DEFAULT_MAX_SIZE;
            }
        }

        private boolean isIgnoreError(final DataStoreParams paramMap) {
            return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.TRUE));
        }

        private String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
            return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                    .get(stream -> stream.map(String::trim).toArray(String[]::new));
        }

        private UrlFilter getUrlFilter(final DataStoreParams paramMap) {
            final UrlFilter urlFilter;
            try {
                urlFilter = ComponentUtil.getComponent(UrlFilter.class);
            } catch (final ComponentNotFoundException e) {
                return null;
            }
            final String include = paramMap.getAsString(INCLUDE_PATTERN);
            if (StringUtil.isNotBlank(include)) {
                urlFilter.addInclude(include);
            }
            final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);
            if (StringUtil.isNotBlank(exclude)) {
                urlFilter.addExclude(exclude);
            }
            urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));
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
