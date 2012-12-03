package com.av.bigdata.storm.efd.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MockedSources;

public class StormTestFixtures {
    static final Logger LOG = LoggerFactory.getLogger(StormTestFixtures.class); // TODO resolve multiple logger bindings
    static final String TOPOLOGY_PREFIX = "StormTopologyName_";
    // static final String LOCAL_TEMP_DIR = "C:\\work\\projects\\bigdata\\batch\\storm\\sample-app-tempdir";
    static final String LOCAL_TEMP_DIR = "C:\\dev\\projects\\bigdata\\storm-sample-app-tempdir";

    // TODO read from storm.itest.properties

    public static String buildTopologyName() {
        return TOPOLOGY_PREFIX + System.currentTimeMillis();
    }

    public static Config buildConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.STORM_LOCAL_DIR, LOCAL_TEMP_DIR);
        config.put(Config.STORM_LOCAL_MODE_ZMQ, false);

        // TODO refactor / read this from corresponding itest.stom.properties
        return config;
    }

    public static CompleteTopologyParam buildCompleteTopologyParam(MockedSources mockedDataSource, Config config) {
        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
        completeTopologyParam.setMockedSources(mockedDataSource);
        completeTopologyParam.setStormConf(config);
        return completeTopologyParam;
    }

    public static void prepareLocalTempDir(boolean initialCleanupRequired) {
        System.setProperty("java.io.tmpdir", LOCAL_TEMP_DIR);

        if (initialCleanupRequired) {
            FileUtils.deleteQuietly(new File(LOCAL_TEMP_DIR));
        }
    }

    public static void shutdownLocalCluster(LocalCluster cluster, String topologyName) {
        try {
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
        catch (Exception e) {
            LOG.warn("LocalCluster has not been gracefully shut down:", e);
        }
    }
}
