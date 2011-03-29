package org.olap4cloud.client;

import java.util.ArrayList;
import java.util.List;

public class CubeQuery {
	
	List<CubeQueryCondition> conditions = new ArrayList<CubeQueryCondition>();
	
	List<CubeQueryAggregate> aggregates = new ArrayList<CubeQueryAggregate>();
	
	List<String> groupBy = new ArrayList<String>();
	
	public CubeQuery() {
		
	}

	public List<CubeQueryCondition> getConditions() {
		return conditions;
	}
	
	public List<CubeQueryAggregate> getAggregates() {
		return aggregates;
	}
	
	public List<String> getGroupBy() {
		return groupBy;
	}
}
