package com.av.bigdata.storm.efd.spout;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import backtype.storm.utils.InprocMessaging;

public class EmailInfoFeederSpoutTest {
	private final Object tuple = Arrays.asList(new Values("auser@domain.com", "127.0.0.1", "login", "timestamp")); 
	
	@Test
	public void emitTupleWhenInprocMessageInjected() {
		EmailInfoFeederSpout spout = new EmailInfoFeederSpout();
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		spout.open(null, null, collector);

		InprocMessaging.sendMessage(spout.getPort(), tuple);
		spout.nextTuple();

		verify(collector).emit((List<Object>) tuple);
	}
	
	@Test
	public void noEmitWhenInprocMessageInjectedToOtherport() {
		EmailInfoFeederSpout spout = new EmailInfoFeederSpout();
		SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
		spout.open(null, null, collector);

		InprocMessaging.sendMessage(spout.getPort() + 1, tuple);
		spout.nextTuple();

		verifyZeroInteractions(collector);
	}
}