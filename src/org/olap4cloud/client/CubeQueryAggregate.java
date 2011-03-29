package org.olap4cloud.client;

public class CubeQueryAggregate {
	String aggregate;

	public void setAggregate(String aggregate) {
		this.aggregate = aggregate;
	}

	public CubeQueryAggregate(String aggregate) {
		this.aggregate = aggregate;
	}
	
	public String getAggregate() {
		return aggregate;
	}
}
