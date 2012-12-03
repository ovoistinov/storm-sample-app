package com.av.bigdata.storm.efd.bolt;

import static org.hamcrest.Matchers.startsWith;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.av.bigdata.storm.efd.domain.ActionInfo;
import com.av.bigdata.storm.efd.domain.ActionInfoFields;
import com.av.bigdata.storm.efd.domain.ActionType;

public class EmailFraudDetectionBolt extends BaseBasicBolt {
    static final Logger LOG = LoggerFactory.getLogger(EmailFraudDetectionBolt.class);

    private final String[] outputFields = new String[] {"alert_message"};
    private Map<String, ActionInfo> actionLog = new HashMap<String, ActionInfo>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        LOG.debug("Executing tuple {}", input);

        try {
            String emailAddress = input.getStringByField(ActionInfoFields.EMAIL.name());
            String ip = input.getStringByField(ActionInfoFields.IP.name());

            String actionTypeValue = input.getStringByField(ActionInfoFields.ACTION_TYPE.name());
            ActionType action = ActionType.valueOf(actionTypeValue != null ? actionTypeValue.toUpperCase() : "UNKNOWN");

            long timestamp = Long.parseLong(input.getStringByField(ActionInfoFields.TIMESTAMP.name()));

            ActionInfo actionInfo = new ActionInfo(emailAddress, ip, action, timestamp);
            LOG.debug("Received ActionInfo: {}", actionInfo);
            
            if (actionInfo != null && detectFraud(actionInfo)) {
                LOG.debug("Email fraud detected: {}", actionInfo);
                collector.emit(new Values("ALERT: fraud detected for " + actionInfo));
            }

            actionLog.put(actionInfo.getEmailAddress(), actionInfo);
        }
        catch (Exception e) {
            LOG.warn("ActionInfo object not created, cannot detect fraud.", e);
        }
    }

    private boolean detectFraud(ActionInfo actionInfo) {
        final ActionInfo cachedActionInfo = actionLog.get(actionInfo.getEmailAddress());
        
        if (cachedActionInfo == null) {
            return false;
        }
        
        return cachedActionInfo != null 
                && cachedActionInfo.getEmailAddress().equals(actionInfo.getEmailAddress()) 
                && cachedActionInfo.getActionType() == actionInfo.getActionType() 
                && !networksMatch(cachedActionInfo.getIp(), actionInfo.getIp());
    }

    private boolean networksMatch(String ip1, String ip2) {
        return ip1 != null 
                && ip2 != null 
                && startsWith(ip1.substring(0, 6)).matches(ip2);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(outputFields));
    }
}