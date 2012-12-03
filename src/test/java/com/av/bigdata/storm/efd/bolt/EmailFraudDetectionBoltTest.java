package com.av.bigdata.storm.efd.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.av.bigdata.storm.efd.domain.ActionInfoFields;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

public class EmailFraudDetectionBoltTest {

    @Test
    public void raiseAlertWhenTwoForDifferentNetworkAndSameEmail() {
        EmailFraudDetectionBolt bolt = new EmailFraudDetectionBolt();
        BasicOutputCollector collector = mock(BasicOutputCollector.class);

        bolt.execute(tupleMockSimple(), collector);
        bolt.execute(tupleMockWithDifferentNetworkAddress(), collector);

        Object alert = "ALERT: fraud detected for ActionInfo [emailAddress=auser@domain.com, ip=101.101.100.100, actionType=LOGIN, timestamp=1354547689586]";
        verify(collector, times(1)).emit((List<Object>) Arrays.asList(alert));
    }

    @Test
    public void noAlertWhenTwoForSameNetworkAndEmail() {
        EmailFraudDetectionBolt bolt = new EmailFraudDetectionBolt();
        BasicOutputCollector collector = mock(BasicOutputCollector.class);

        bolt.execute(tupleMockSimple(), collector);
        bolt.execute(tupleMockSimple(), collector);

        verifyZeroInteractions(collector);
    }
    
    private Tuple tupleMockSimple() {
        Tuple result = mock(Tuple.class);
        when(result.getStringByField(ActionInfoFields.EMAIL.fieldName())).thenReturn("auser@domain.com");
        when(result.getStringByField(ActionInfoFields.IP.fieldName())).thenReturn("100.100.100.100");
        when(result.getStringByField(ActionInfoFields.ACTION_TYPE.fieldName())).thenReturn("login");
        when(result.getStringByField(ActionInfoFields.TIMESTAMP.fieldName())).thenReturn("1354547679586");
        return result;
    }
    
    private Tuple tupleMockWithDifferentNetworkAddress() {
        Tuple result = mock(Tuple.class);
        when(result.getStringByField(ActionInfoFields.EMAIL.fieldName())).thenReturn("auser@domain.com");
        when(result.getStringByField(ActionInfoFields.IP.fieldName())).thenReturn("101.101.100.100");
        when(result.getStringByField(ActionInfoFields.ACTION_TYPE.fieldName())).thenReturn("login");
        when(result.getStringByField(ActionInfoFields.TIMESTAMP.fieldName())).thenReturn("1354547689586");
        return result;
    }
}