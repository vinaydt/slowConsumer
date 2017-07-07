/**
 * Put your copyright and license info here.
 */
package com.example.SlowConsumer;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

/**
 * This is a simple operator that emits random number.
 */
public class RGCopy extends BaseOperator implements InputOperator, Partitioner<RGCopy> {

	public int noOfPartitions = 3;
	public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

	@Override
	public void emitTuples() {
		out.emit(Math.random());
	}

	@Override
	public Collection<Partition<RGCopy>> definePartitions(
			Collection<Partition<RGCopy>> listOfPartitions, PartitioningContext context) {
		//System.out.println("-----1. RG No of partitions is -----" + listOfPartitions.size());

		Kryo kryo = new Kryo();

		List<Partition<RGCopy>> newPartitions = Lists.newArrayListWithExpectedSize(noOfPartitions);

		for (int i = 0; i < noOfPartitions; i++) {
			RGCopy oper = cloneObject(kryo, this);
			newPartitions.add(new DefaultPartition<>(oper));
		}

		//System.out.println("-----1. RG No of partitions is -----" + newPartitions.size());
		return newPartitions;
	}

	@Override
	public void partitioned(Map<Integer, Partition<RGCopy>> mapOfPartitions) {
		//System.out.println("-----2. RG No of partitions is -----" + mapOfPartitions.size());
		if (this.noOfPartitions != mapOfPartitions.size()) {
			throw new RuntimeException("Size mismatch!!");
		}
	}

	@SuppressWarnings("unchecked")
	private static <SRC> SRC cloneObject(Kryo kryo, SRC src) {
		kryo.setClassLoader(src.getClass().getClassLoader());
		ByteArrayOutputStream bos = null;
		Output output;
		Input input = null;
		try {
			bos = new ByteArrayOutputStream();
			output = new Output(bos);
			kryo.writeObject(output, src);
			output.close();
			input = new Input(bos.toByteArray());
			return (SRC) kryo.readObject(input, src.getClass());
		} finally {
			IOUtils.closeQuietly(input);
			IOUtils.closeQuietly(bos);
		}
	}
}
