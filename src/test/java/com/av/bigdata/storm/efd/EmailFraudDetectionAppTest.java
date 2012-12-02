package com.av.bigdata.storm.efd;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.av.bigdata.storm.efd.bolt.EmailFraudDetectionBolt;
import com.av.bigdata.storm.efd.spout.EmailInfoFeederSpout;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class EmailFraudDetectionAppTest {
    private LocalCluster cluster;
    private String topologyName;
    
    //@Before
    public void setUp() {
        this.cluster = new LocalCluster();
        this.topologyName = "topology_name_" + System.currentTimeMillis(); 
    }
    
    //@After
    public void tearDown() {
        this.cluster.killTopology(topologyName);
        this.cluster.shutdown();
    }
    
    //@Test
    public void whenTwoLoginsFromSingleEmailThenEmailFraudAlertGenerated() {
        // TODO implement configuration helper
        //StormTopology topology = new TestTopologyBuilder().build();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new EmailInfoFeederSpout(), 3);
        builder.setBolt("2", new EmailFraudDetectionBolt(), 4).fieldsGrouping("1", new Fields("email"));
        
        Config topologyConfig = new Config();
        
        topologyConfig.setDebug(true);
        topologyConfig.put(Config.STORM_ZOOKEEPER_PORT, 8082); // TODO refactor with getPort method
        topologyConfig.put(Config.NIMBUS_THRIFT_PORT, 8083); // TODO refactor with getPort method
        
        cluster.submitTopology(topologyName, topologyConfig, builder.createTopology());
        
        // TODO implement extraction of test data and assertion against expected data
        
        assertThat(true, is(false));
    }
    
    //@Test
    public void whenTwoLoginsFromDIfferentEmailThenNoEmailFraudAlertGenerated() {
        // TODO implement configuration helper
        // TODO: implement this scenario
        fail();
    }
    
//    private StormTopology buildTopology() {
//        return null;
//    }
    
    //@Test
    public void testSomething() {
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.setDebug(true);
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			//@Override
			public void run(ILocalCluster cluster) {
				// build the test topology
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("1", new EmailInfoFeederSpout(), 3);
				StormTopology topology = builder.createTopology();

				// complete the topology

				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData("1", new Values("email@email.com", "127.0.0.1", "login"));

				// prepare the config
				Config conf = new Config();
				conf.setNumWorkers(2);

				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology,
						completeTopologyParam);

				// check whether the result is right
				assertTrue(Testing.multiseteq(new Values(new Values("nathan"),
						new Values("bob"), new Values("joey"), new Values(
								"nathan")), Testing.readTuples(result, "1")));
				assertTrue(Testing.multiseteq(new Values(new Values("nathan", 1),
						new Values("nathan", 2), new Values("bob", 1),
						new Values("joey", 1)), Testing.readTuples(result, "2")));
				assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
						new Values(3), new Values(4)), Testing.readTuples(
						result, "3")));
				assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2),
						new Values(3), new Values(4)), Testing.readTuples(
						result, "4")));
			}

		});
    }
    
    @Test
    public void testNewTopology() throws Exception {
    	String oldTempDir = System.getProperty("java.io.tmpdir");
    	System.setProperty("java.io.tmpdir", "C:\\work\\projects\\bigdata\\batch\\storm\\sample-app-tempdir");
    	
    	final Config config = new Config();
    	config.setDebug(true);
    	config.put(Config.STORM_LOCAL_DIR, System.getProperty("java.io.tmpdir"));
    	
    	LocalCluster cluster = new LocalCluster();
    	topologyName = "test" + System.currentTimeMillis();
    	
    	
    	TopologyBuilder topology = new TopologyBuilder();
    	topology.setSpout("email_spout", new EmailInfoFeederSpout(), 1);
    	
    	cluster.submitTopology(topologyName, config, topology.createTopology());
    	await().atMost(10, SECONDS);
    	
    	cluster.killTopology(topologyName);
    	try {
    		cluster.shutdown();
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	
    	System.setProperty("java.io.tmpdir", oldTempDir);
    }
    
    private int getFreePort() throws IOException
    {
        ServerSocket socket = new ServerSocket(0);

        int port = socket.getLocalPort();

        socket.close();

        return port;
    }
    
}
