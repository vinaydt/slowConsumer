/**
 * Put your copyright and license info here.
 */
package com.example.SlowConsumer;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator {

/*	@Override
	public void setup(OperatorContext context) {
		super.setup(context);
	}

	@Override
	public void beginWindow(long windowId) {
		StatelessPartitioner<RandomNumberGenerator> sp = new StatelessPartitioner<RandomNumberGenerator>();
		sp.setPartitionCount(noOfPartitions);
		System.out.println("RG::BeginWindow No of paritions are:" + sp.getPartitionCount());
		super.beginWindow(windowId);
	}*/

	public final int noOfPartitions = 5;
	public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

	@Override
	public void emitTuples() {
		out.emit(Math.random());
	}

}
