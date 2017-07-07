/**
 * Put your copyright and license info here.
 */
package com.example.SlowConsumer;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "MyFirstApplication")
public class Application implements StreamingApplication {

	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		// Sample DAG with 2 operators
		// Replace this code with the DAG you want to build

		RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);

		CustomOperator co = dag.addOperator("console", new CustomOperator());

		dag.addStream("randomData", randomGenerator.out, co.input);

	}
}
