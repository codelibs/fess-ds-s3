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
import org.codelibs.core.io.ResourceUtil;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static cloud.localstack.TestUtils.*;
import static org.junit.Assert.fail;

class TestUtils {

    static final String BUCKET_NAME = "fess";
    static final String[] FILES = { "sample-0.txt", "sample-1.txt" };

    static void initializeBuckets() {
        resetBuckets();
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
