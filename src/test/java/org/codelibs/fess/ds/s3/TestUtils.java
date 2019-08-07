package org.codelibs.fess.ds.s3;

import cloud.localstack.Localstack;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;

import java.util.HashMap;
import java.util.Map;

import static cloud.localstack.TestUtils.*;

class TestUtils {

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
