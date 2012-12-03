package com.av.bigdata.storm.efd.spout;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.InprocMessaging;

import com.av.bigdata.storm.efd.domain.ActionInfoFields;

public class EmailInfoFeederSpout extends BaseRichSpout {
	static final Logger LOG = LoggerFactory.getLogger(EmailInfoFeederSpout.class);
    private SpoutOutputCollector collector;
    private int port;

    public EmailInfoFeederSpout() {
        this.port = InprocMessaging.acquireNewPort();
    }

    public void feed(List<Object> tuple) {
        InprocMessaging.sendMessage(port, tuple);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        List<Object> tuple = (List<Object>) InprocMessaging.pollMessage(port);
        if (tuple != null) {
            LOG.debug("Emitting tuple: {}" + tuple.toString());
            collector.emit(tuple);
            return;
        }

        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ActionInfoFields.fieldNames()));
    }

    public int getPort() {
        return this.port;
    }
}
