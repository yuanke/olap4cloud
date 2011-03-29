package org.olap4cloud.test;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQuery;
import org.olap4cloud.client.CubeQueryAggregate;
import org.olap4cloud.client.CubeQueryCondition;
import org.olap4cloud.client.CubeQueryResult;
import org.olap4cloud.client.CubeQueryResultRow;
import org.olap4cloud.client.OLAPEngine;
import org.olap4cloud.impl.aggr.SumCubeScanAggregate;

public class OLAPEngineTest {
	
	public static void main(String[] args) throws Exception {
		executeQueryTest();
	}
	
	static void executeQueryTest() throws Exception {
		CubeDescriptor cubeDescriptor = TestCubeUtils.createTestCubeDescriptor();
		cubeDescriptor.getAggregationCubes();
		CubeQuery cubeQuery = new CubeQuery();
		CubeQueryCondition condition = new CubeQueryCondition("d3");
		condition.getDimensionValues().add(1l);
		cubeQuery.getConditions().add(condition);
		condition = new CubeQueryCondition("d1");
		condition.getDimensionValues().add(0l);
		cubeQuery.getConditions().add(condition);
//		condition = new CubeQueryCondition("d2");
//		condition.getDimensionValues().add(1l);
//		cubeQuery.getConditions().add(condition);
		cubeQuery.getAggregates().add(new CubeQueryAggregate("min(m1)"));
		cubeQuery.getAggregates().add(new CubeQueryAggregate("max(m2)"));
		cubeQuery.getAggregates().add(new CubeQueryAggregate("count(m3)"));
		cubeQuery.getGroupBy().add("d1");
		OLAPEngine olapEngine = new OLAPEngine();
		CubeQueryResult r = olapEngine.executeQuery(cubeQuery, cubeDescriptor);
		for(CubeQueryResultRow row: r.getRows()) {
			for(long l: row.getGroupBy())
				System.out.print(l + "\t");
			for(double d: row.getValues())
				System.out.print(d + "\t");
			System.out.println();
		}
	}
}
