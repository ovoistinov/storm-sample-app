package com.av.bigdata.batch.storm.sample.app.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class EmailInfoFeederSpout extends BaseRichSpout  {
    private static final long serialVersionUID = 6156995191920101152L;
    private SpoutOutputCollector collector;
    private final String[] emailAddresses = new String[] {
            "auser@domain.com", 
            "buser@domain.com"
    };
    
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        Utils.sleep(100);
        
        Random r = new Random();
        String tupleValue = emailAddresses[r.nextInt(emailAddresses.length)];
        collector.emit(new Values(tupleValue));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(getFields()));
    }
    
    private String[] getFields() {
        return new String[]{
                "email", 
                "address", 
                "action"
        };
    }
}
