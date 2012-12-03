package com.av.bigdata.storm.efd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
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
import com.av.bigdata.storm.efd.domain.ActionInfoFields;
import com.av.bigdata.storm.efd.spout.EmailInfoFeederSpout;
import com.av.bigdata.storm.efd.test.BaseStormIntegrationTest;
import com.av.bigdata.storm.efd.test.StormTestHelper;

public class EmailFraudDetectorIntegrationTest extends BaseStormIntegrationTest {
    static final Logger LOG = LoggerFactory.getLogger(EmailFraudDetectorIntegrationTest.class);

    @Test
    public void testEmailFraudDetectionTopology() {
    	// Given
        TopologyBuilder topology = new TopologyBuilder();
        topology.setSpout("email_spout", new EmailInfoFeederSpout(), 2);
		topology.setBolt("fraud_detection_bolt", new EmailFraudDetectionBolt(), 2).fieldsGrouping(
				"email_spout", 
				new Fields(ActionInfoFields.EMAIL.fieldName()));
		// and
        MockedSources mockedDataSource = new MockedSources();
        mockedDataSource.addMockData("email_spout", new Values("auser@gmail.com", "100.100.100.100", "login", "12345"));
        mockedDataSource.addMockData("email_spout", new Values("buser@gmail.com", "100.100.100.100", "login", "12345"));
        mockedDataSource.addMockData("email_spout", new Values("auser@gmail.com", "101.101.100.100", "login", "456"));

        // When
        Map result =
                Testing.completeTopology(getLocalCluster(), topology.createTopology(),
                        StormTestHelper.buildCompleteTopologyParam(mockedDataSource, getConfig()));
        
        // Then
        List<Values> actual = new ArrayList<Values>(Testing.readTuples(result, "fraud_detection_bolt"));
        assertThat((String) actual.get(0).get(0), is("ALERT: fraud detected for ActionInfo [emailAddress=auser@gmail.com, ip=101.101.100.100, actionType=LOGIN, timestamp=456]"));
    }
}
