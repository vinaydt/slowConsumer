/**
 * Put your copyright and license info here.
 */
package com.example.SlowConsumer;

import org.apache.commons.lang.RandomStringUtils;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomStringGenerator extends BaseOperator implements InputOperator {

	public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

	private int count = 0;
	
	private int maxTuplesPerWindow = 50000;
	
	private int messageSize = 100;

	public int getMaxTuplesPerWindow() {
		return maxTuplesPerWindow;
	}

	public void setMaxTuplesPerWindow(int maxTuplesPerWindow) {
		this.maxTuplesPerWindow = maxTuplesPerWindow;
	}

	public int getMessageSize() {
		return messageSize;
	}

	public void setMessageSize(int messageSize) {
		this.messageSize = messageSize;
	}

	
	@Override
	public void beginWindow(long windowId) {
		count = 0;
	}
	
	@Override
	public void emitTuples() {
		if(count++<maxTuplesPerWindow)
			out.emit(RandomStringUtils.randomAlphabetic(messageSize));
	}

}
