package org.olap4cloud.client;

import java.io.Serializable;

public class CubeDimension implements Serializable {

	String name;
	
	public CubeDimension(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
