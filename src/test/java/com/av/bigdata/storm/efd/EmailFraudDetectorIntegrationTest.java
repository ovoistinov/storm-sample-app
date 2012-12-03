package com.av.bigdata.storm.efd;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Testing;
import backtype.storm.testing.MockedSources;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.av.bigdata.storm.efd.bolt.EmailFraudDetectionBolt;
import com.av.bigdata.storm.efd.spout.EmailInfoFeederSpout;
import com.av.bigdata.storm.efd.test.BaseStormIntegrationTest;
import com.av.bigdata.storm.efd.test.StormTestFixtures;

public class EmailFraudDetectorIntegrationTest extends BaseStormIntegrationTest {
    static final Logger LOG = LoggerFactory.getLogger(EmailFraudDetectorIntegrationTest.class);

    @Test
    public void testEmailFraudDetectionTopology() {
        final TopologyBuilder topology = new TopologyBuilder();
        topology.setSpout("email_spout", new EmailInfoFeederSpout(), 2);
        topology.setBolt("fraud_detection_bolt", new EmailFraudDetectionBolt(), 2).fieldsGrouping("email_spout", new Fields("email"));

        MockedSources mockedDataSource = new MockedSources();
        mockedDataSource.addMockData("email_spout", new Values("a@gmail.com", "100.100.100.100", "login", "12345"));
        mockedDataSource.addMockData("email_spout", new Values("b@gmail.com", "100.100.100.100", "login", "12345"));
        mockedDataSource.addMockData("email_spout", new Values("a@gmail.com", "100.100.100.100", "login", "456"));

        Map testResults =
                Testing.completeTopology(getLocalCluster(), topology.createTopology(),
                        StormTestFixtures.buildCompleteTopologyParam(mockedDataSource, getConfig()));

        await().atMost(5, SECONDS);

        Testing.readTuples(testResults, "fraud_detection_bolt");
        Testing.readTuples(testResults, "email_spout");

        assertThat(testResults, is(notNullValue()));
        assertThat(testResults.size(), greaterThan(0));

        LOG.debug("some testing");
        LOG.info("Result map: {}", testResults.toString());
    }
}
