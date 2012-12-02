package com.av.bigdata.storm.efd;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Testing;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.av.bigdata.storm.efd.bolt.EmailFraudDetectionBolt;
import com.av.bigdata.storm.efd.spout.EmailInfoFeederSpout;
import com.av.bigdata.storm.efd.test.BaseStormIntegrationTest;

public class EmailFraudDetectorIntegrationTest extends BaseStormIntegrationTest {
	static final Logger LOG = LoggerFactory.getLogger(EmailFraudDetectorIntegrationTest.class);
	
	@Test
	public void testEmailFraudDetectionTopology() {
    	final TopologyBuilder topology = new TopologyBuilder();
    	topology.setSpout("email_spout", new EmailInfoFeederSpout(), 2);
    	topology.setBolt("fraud_detection_bolt", new EmailFraudDetectionBolt(), 2).fieldsGrouping("email_spout", new Fields("email"));

    	MockedSources mockedSources = new MockedSources();
    	mockedSources.addMockData("email_spout", new Values("a@gmail.com", "100.100.100.100", "login", "12345"));
    	
    	//getLocalCluster().submitTopology(getTopologyName(), getConfig(), topology.createTopology());
		CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
		completeTopologyParam.setMockedSources(mockedSources);
		completeTopologyParam.setStormConf(getConfig());
    	
    	Map result = Testing.completeTopology(getLocalCluster(), topology.createTopology(), completeTopologyParam);
    	
    	await().atMost(5, SECONDS);
    	
    	Testing.readTuples(result, "fraud_detection_bolt");
    	Testing.readTuples(result, "email_spout");

    	//assertThat(result, is(notNullValue()));
    	//assertThat(result.size(), greaterThan(0));
    	
    	LOG.debug("some testing");
    	LOG.info("Result map: {}", result.toString());
	}
	
	
//	MkClusterParam mkClusterParam = new MkClusterParam();
//	mkClusterParam.setSupervisors(4);
//	mkClusterParam.setDaemonConf(getConfig());
//	
//	Testing.withLocalCluster(mkClusterParam, new TestJob() {
//    	@Override
//    	public void run (ILocalCluster cluster) {
//	    	MockedSources mockedSources = new MockedSources();
//	    	mockedSources.addMockData("email_spout", new Values("a@gmail.com", "100.100.100.100", "login", "12345"));
//	    	
//			CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
//			completeTopologyParam.setMockedSources(mockedSources);
//			completeTopologyParam.setStormConf(getConfig());
//	    	
//	    	Map result = Testing.completeTopology(getLocalCluster(), topology.createTopology(), completeTopologyParam);
//	    	Testing.readTuples(result, "fraud_detection_bolt");
//	    	Testing.readTuples(result, "email_spout");
//	    	
//	    	LOG.debug("some testing");
//	    	LOG.info("Result map: {}", result.toString());
//	    	
//	    	assertThat(result, is(notNullValue()));
//    	}
//	});
	
}
