package com.av.bigdata.storm.efd.test;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;


/**
 * The class defines default implementation of the <code>LocalCluster</code> initialization and shut down.
 * 
 * It also provides workaround for not deleted ZooKeeper log file from default java temp directory 
 * when shutting down cluster on Windows. This is a known bug (TODO provide link to bug description)
 * 
 * @author avoistinov
 * @date   30.11.2012
 */
public class BaseStormIntegrationTest {
	protected final Logger LOG = LoggerFactory.getLogger(BaseStormIntegrationTest.class); // TODO resolve multiple logger bindings
	
	private final String DEFAULT_LOCAL_TEMP_DIR = System.getProperty("java.io.tmpdir");

	private Config config;
	private LocalCluster cluster;
	private String topologyName;

	@Before
	public void startUp() throws Exception {
		StormTestFixtures.prepareLocalTempDir(true);

		topologyName = StormTestFixtures.buildTopologyName();
		LOG.debug("Topology name is {}", topologyName);
		
		StormTestFixtures.buildConfig();
		cluster = new LocalCluster();
	}

	@After
	public void tearDown() {
		System.setProperty("java.io.tmpdir", DEFAULT_LOCAL_TEMP_DIR);
		StormTestFixtures.shutdownLocalCluster(getLocalCluster(), getTopologyName());
	}
	
	protected LocalCluster getLocalCluster() {
		return this.cluster;
	}
	
	protected String getTopologyName() {
		return this.topologyName;
	}
	
	protected Config getConfig() {
		return this.config;
	}
}
