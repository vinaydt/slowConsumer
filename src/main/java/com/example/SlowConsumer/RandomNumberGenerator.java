/**
 * Put your copyright and license info here.
 */
package com.example.SlowConsumer;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator {

	public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

	private int count = 0;
	
	private int maxTuplesPerWindow = 50000;
	
	@Override
	public void beginWindow(long windowId) {
		count = 0;
	}
	
	@Override
	public void emitTuples() {
		if(count++<maxTuplesPerWindow)
			out.emit(Double.toString(Math.random()));
	}

}
