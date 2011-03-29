package org.olap4cloud.client;

import java.util.ArrayList;
import java.util.List;

public class CubeQueryResultRow {
	
	List<Long> groupBy = new ArrayList<Long>();
	
	List<Double> values = new ArrayList<Double>();
	
	public CubeQueryResultRow() {
		
	}
	
	public List<Long> getGroupBy() {
		return groupBy;
	}

	public List<Double> getValues() {
		return values;
	}
}
