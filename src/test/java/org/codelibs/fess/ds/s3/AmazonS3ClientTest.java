package org.codelibs.fess.ds.s3;

import org.dbflute.utflute.lastaflute.LastaFluteTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;

public class AmazonS3ClientTest extends LastaFluteTestCase {

    private static final Logger logger = LoggerFactory.getLogger(AmazonS3ClientTest.class);

    private static final String ACCESS_KEY_ID = "";
    private static final String SECRET_KEY = "";
    private static final String REGION = Region.AP_NORTHEAST_1.id();

    private AmazonS3Client client;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Map<String, String> params = new HashMap<>();
        params.put(AmazonS3Client.ACCESS_KEY_ID, ACCESS_KEY_ID);
        params.put(AmazonS3Client.SECRET_KEY, SECRET_KEY);
        params.put(AmazonS3Client.REGION, REGION);
        client = new AmazonS3Client(params);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void test() {
        // getBuckets();
        // getObjects();
    }

    private void getBuckets() {
        client.getBuckets(bucket -> {
            logger.debug(bucket.name());
        });
    }

    private void getObjects() {
        client.getBuckets(bucket -> {
            client.getObjects(bucket.name(), obj -> {
                logger.debug(obj.key());
            });
        });
    }

}
