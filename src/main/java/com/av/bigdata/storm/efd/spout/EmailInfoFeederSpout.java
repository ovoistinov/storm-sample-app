package com.av.bigdata.storm.efd.spout;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.InprocMessaging;

public class EmailInfoFeederSpout extends BaseRichSpout  {
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
    		collector.emit(tuple);
    		return;
    	}
    	
    	try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
    	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(getFields()));
    }
    
    public int getPort() {
    	return this.port;
    }
    
    private String[] getFields() {
        return new String[]{"email", "address", "action", "timestamp"}; // TODO put into field definition enum of class
    }
}
