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
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.commons.io.IOUtils;
import org.codelibs.core.io.ResourceUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static cloud.localstack.TestUtils.*;
import static org.junit.Assert.fail;

class TestUtils {

    static final String[] BUCKETS = { "fess-0", "fess-1" };
    static final String[] PATHS = { "files/sample-0.txt", "files/sample-1.txt" };
    static final Map<String, String> FILE_MAP = new LinkedHashMap<>();

    static {
        for (final String path : PATHS) {
            try {
                FILE_MAP.put(path, IOUtils.toString(ResourceUtil.getResourceAsStream(path), StandardCharsets.UTF_8));
            } catch (final IOException e) {
                fail(e.getMessage());
            }
        }
    }

    static void initializeBuckets() {
        resetBuckets();
        final AmazonS3 s3 = getClientS3();
        for (final String bucketName : BUCKETS) {
            final Bucket bucket = s3.createBucket(bucketName);
            for (final String path : PATHS) {
                try {
                    final File file = ResourceUtil.getResourceAsFile(path);
                    s3.putObject(bucket.getName(), path, file);
                } catch (final Exception e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    static void resetBuckets() {
        final AmazonS3 s3 = getClientS3();
        s3.listBuckets().forEach(bucket -> {
            ObjectListing objectListing = s3.listObjects(bucket.getName());
            while (true) {
                objectListing.getObjectSummaries().forEach(object -> {
                    s3.deleteObject(bucket.getName(), object.getKey());
                });

                if (objectListing.isTruncated()) {
                    objectListing = s3.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            s3.deleteBucket(bucket.getName());
        });
    }

    static AmazonS3Client getClient() {
        return new AmazonS3Client(getParams());
    }

    static Map<String, String> getParams() {
        final Map<String, String> params = new HashMap<>();
        params.put(AmazonS3Client.ACCESS_KEY_ID, TEST_ACCESS_KEY);
        params.put(AmazonS3Client.SECRET_KEY, TEST_SECRET_KEY);
        params.put(AmazonS3Client.REGION, DEFAULT_REGION);
        params.put(AmazonS3Client.ENDPOINT, Localstack.getEndpointS3());
        return params;
    }

}
