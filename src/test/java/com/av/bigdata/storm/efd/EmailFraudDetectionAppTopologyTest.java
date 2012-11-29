package com.av.bigdata.batch.storm.sample.app;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.av.bigdata.batch.storm.sample.app.bolt.FraudDetectionBolt;
import com.av.bigdata.batch.storm.sample.app.spout.EmailEmissionSpout;
import com.av.bigdata.batch.storm.sample.app.util.TestTopologyBuilder;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class EmailFraudDetectionAppTopologyTest {
    private LocalCluster cluster;
    private String topologyName;
    
    @Before
    public void setUp() {
        this.cluster = new LocalCluster();
        this.topologyName = "topology_name_" + System.currentTimeMillis(); 
    }
    
    @After
    public void tearDown() {
        this.cluster.killTopology(topologyName);
        this.cluster.shutdown();
    }
    
    @Test
    public void whenTwoLoginsFromSingleEmailThenEmailFraudAlertGenerated() {
        // TODO implement configuration helper
        //StormTopology topology = new TestTopologyBuilder().build();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new EmailEmissionSpout(), 3);
        builder.setBolt("2", new FraudDetectionBolt(), 4).fieldsGrouping("1", new Fields("email"));
        
        Config topologyConfig = new Config();
        
        topologyConfig.setDebug(true);
        topologyConfig.put(Config.STORM_ZOOKEEPER_PORT, 8082); // TODO refactor with getPort method
        topologyConfig.put(Config.NIMBUS_THRIFT_PORT, 8083); // TODO refactor with getPort method
        
        cluster.submitTopology(topologyName, topologyConfig, builder.createTopology());
        
        // TODO implement extraction of test data and assertion against expected data
        
        assertThat(true, is(false));
    }
    
    @Test
    public void whenTwoLoginsFromDIfferentEmailThenNoEmailFraudAlertGenerated() {
        // TODO implement configuration helper
        // TODO: implement this scenario
        fail();
    }
    
//    private StormTopology buildTopology() {
//        return null;
//    }
}
