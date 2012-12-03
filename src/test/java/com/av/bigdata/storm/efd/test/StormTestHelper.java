package com.av.bigdata.storm.efd.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MockedSources;

public class StormTestHelper {
	static final Logger LOG = LoggerFactory.getLogger(StormTestHelper.class);
	static final String TOPOLOGY_PREFIX = "StormTopologyName_";
	static final String LOCAL_TEMP_DIR = "C:\\work\\projects\\bigdata\\batch\\storm\\sample-app-tempdir";

	public static String buildTopologyName() {
		return TOPOLOGY_PREFIX + System.currentTimeMillis();
	}

	public static Config buildConfig() {
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(2);
		config.put(Config.TOPOLOGY_WORKERS, 2);
		config.put(Config.STORM_LOCAL_DIR, LOCAL_TEMP_DIR);
		config.put(Config.STORM_LOCAL_MODE_ZMQ, false);

		LOG.debug("Build LocalCluster configuration: {}", config);

		return config;
	}

	public static CompleteTopologyParam buildCompleteTopologyParam(
			MockedSources mockedDataSource, Config config) {
		LOG.debug("{}", mockedDataSource);
		LOG.debug("{}", config);

		CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
		completeTopologyParam.setMockedSources(mockedDataSource);
		completeTopologyParam.setStormConf(config);

		LOG.debug("Build Complete topology parameters: {}",
				completeTopologyParam);

		return completeTopologyParam;
	}

	public static void prepareLocalTempDir(boolean initialCleanupRequired) {
		System.setProperty("java.io.tmpdir", LOCAL_TEMP_DIR);

		if (initialCleanupRequired) {
			FileUtils.deleteQuietly(new File(LOCAL_TEMP_DIR));
		}
	}

	public static void shutdownLocalCluster(LocalCluster cluster,
			String topologyName) {
		try {
			LOG.debug("Killing topology {}", topologyName);
			cluster.killTopology(topologyName);

			LOG.debug("Shutting down cluster {}", cluster);
			cluster.shutdown();
		} catch (Exception e) {
			LOG.warn("LocalCluster has not been gracefully shut down:", e);
		}
	}
}
