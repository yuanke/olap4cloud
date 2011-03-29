package org.olap4cloud.client;

import java.util.ArrayList;
import java.util.List;

public class CubeQueryResult {

	List<CubeQueryResultRow> rows = new ArrayList<CubeQueryResultRow>();
	
	public CubeQueryResult() {
		
	}
	
	public List<CubeQueryResultRow> getRows() {
		return rows;
	}
}
