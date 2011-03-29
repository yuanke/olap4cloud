package org.olap4cloud.impl.aggr;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.OLAPEngineException;
import org.olap4cloud.impl.CubeScanAggregate;

public class SumCubeScanAggregate extends CubeScanAggregate {

	double value = 0;
	
	public SumCubeScanAggregate(String s, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		super(s, cubeDescriptor);
	}
	
	public void collect(double v) {
		value += v;		
	}

	@Override
	public double getResult() {
		return value;
	}

	@Override
	public void reset() {
		value = 0;
	}

	@Override
	public void combine(double v) {
		collect(v);
	}

	@Override
	public void reduce(double v) {
		collect(v);
	}

}
