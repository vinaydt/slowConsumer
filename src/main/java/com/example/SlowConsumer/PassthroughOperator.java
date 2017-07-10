package com.example.SlowConsumer;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class PassthroughOperator extends BaseOperator {
	
	private int groupOfTuples = 1000;
	private int delayInterval = 1;
	private int count = 0;
	
	public int getDelayInterval() {
		return this.delayInterval;
	}

	public void setDelayInterval(int delayInterval) {
		this.delayInterval = delayInterval;
	}

	public int getNoOfTuples() {
		return this.groupOfTuples;
	}

	@Override
	public void beginWindow(long windowId) {
		count = 0;
	}
	
	protected void slowConsumer(String s) throws InterruptedException {
		if (count++ % getNoOfTuples() == 0)
			Thread.sleep(getDelayInterval());

	}
	
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String tuple) {
			try {
				slowConsumer(tuple);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			output.emit(tuple);
		}

	};
	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
}
