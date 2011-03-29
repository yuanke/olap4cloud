package org.olap4cloud.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class CubeScan implements Serializable {
	
	static Logger logger = Logger.getLogger(CubeScan.class);
	
	List<Pair<byte[], byte[]>> ranges = new ArrayList<Pair<byte[],byte[]>>();
	
	
	List<CubeScanCondition> conditions = new ArrayList<CubeScanCondition>();
	
	List<CubeScanAggregate> cubeScanAggregates = new ArrayList<CubeScanAggregate>();
	
	List<Pair<byte[], byte[]>> columns = null;
	
	int groupBy[];

	public CubeScan() {
		
	}

	public List<CubeScanCondition> getConditions() {
		return conditions;
	}
	
	public List<Pair<byte[], byte[]>> getRanges() {
		return ranges;
	}
	
	public Scan getHBaseScan() {
		String methodName = "getHBaseScan() ";
		Scan scan = new Scan();
		if(ranges.size() > 0) {
			byte minRow[] = null;
			byte maxRow[] = null;
			for(Pair<byte[], byte[]> r: ranges) {
				if(minRow == null || DataUtils.compareRowKeys(minRow, r.getFirst()) > 0)
					minRow = r.getFirst();
				if(maxRow == null || DataUtils.compareRowKeys(maxRow, r.getSecond()) < 0)
					maxRow = r.getSecond();
			}
			if(logger.isDebugEnabled()) logger.debug(methodName + "add range to HBase scan: ["
					+ LogUtils.describe(minRow) + ", " + LogUtils.describe(maxRow) + "]");
			scan.setStartRow(minRow);
			scan.setStopRow(maxRow);
		}
		scan.setCaching(10000);
		return scan;
	}
	
	public List<CubeScanAggregate> getCubeScanAggregates() {
		return cubeScanAggregates;
	}
	
	public void prepare() {
		columns = new ArrayList<Pair<byte[],byte[]>>();
		HashSet<Pair<byte[], byte[]>> columnsSet = new HashSet<Pair<byte[],byte[]>>();
		for(CubeScanAggregate aggregate:  getCubeScanAggregates()) 
			columnsSet.add(aggregate.getColumn());
		columns.addAll(columnsSet);
		for(int i = 0; i < columns.size(); i ++)
			for(CubeScanAggregate aggregate:  getCubeScanAggregates())
				if(columns.get(i).equals(aggregate.getColumn()))
					aggregate.setColumnNumber(i);
	}
	
	public List<Pair<byte[], byte[]>> getColumns() {
		return columns;
	}
	
	public int[] getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(int[] groupBy) {
		this.groupBy = groupBy;
	}
}
