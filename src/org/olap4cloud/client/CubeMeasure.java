package org.olap4cloud.client;

import java.io.Serializable;

public class CubeMeasure implements Serializable {

	String name;
	
	public CubeMeasure(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
