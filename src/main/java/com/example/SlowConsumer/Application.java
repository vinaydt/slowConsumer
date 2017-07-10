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
		// Sample DAG with 3 operators
		// Replace this code with the DAG you want to build

		RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
		
		PassthroughOperator passthroughOperator = dag.addOperator("passthrough", new PassthroughOperator());

		CustomOperator co = dag.addOperator("console", new CustomOperator());

		dag.addStream("randomData", randomGenerator.out, passthroughOperator.input);
		
		dag.addStream("passthrough", passthroughOperator.output, co.input);

	}
}
