package org.olap4cloud.impl;

import java.io.Serializable;

public class CubeScanCondition implements Serializable {
	
	private static final long serialVersionUID = -5655842241556062368L;

	int dimensionNumber = 0;
	
	public int getDimensionNumber() {
		return dimensionNumber;
	}

	public long[] getValues() {
		return values;
	}

	long values[];
	
	public CubeScanCondition(int dimensionNumber, long values[]) {
		this.dimensionNumber = dimensionNumber;
		this.values = values;
	}
	
	
}
