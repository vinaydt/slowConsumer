package com.example.SlowConsumer;

import java.io.PrintStream;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class CustomOperator extends BaseOperator {

	private transient PrintStream stream = System.out;

	private boolean print = false;
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

	public void setGroupOfTuples(int groupOfTuples) {
		this.groupOfTuples = groupOfTuples;
	}

	@Override
	public void beginWindow(long windowId) {
		count = 0;
	}

	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String t) {
			try {
				slowConsumer(t);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	};

	protected void slowConsumer(String s) throws InterruptedException {
		if (count++ % getNoOfTuples() == 0)
			Thread.sleep(getDelayInterval());

		if (print)
			stream.println(s);
	}
}
