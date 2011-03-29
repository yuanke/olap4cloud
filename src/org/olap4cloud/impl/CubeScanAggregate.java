package org.olap4cloud.impl;

import java.io.Serializable;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeDimension;
import org.olap4cloud.client.CubeMeasure;
import org.olap4cloud.client.OLAPEngineException;

public abstract class CubeScanAggregate implements Serializable {
	
	private static final long serialVersionUID = -9180589182756514579L;

	static Logger logger = Logger.getLogger(CubeScanAggregate.class);
	
	Pair<byte[], byte[]> column;
	
	String aggregateName;
	
	int columnNumber = -1;
	
	String columnName;
	
	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}

	public CubeScanAggregate(String aggregate, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		String methodName = "constructor() ";
		StringTokenizer st = new StringTokenizer(aggregate, "()", false);
		aggregateName = st.nextToken();
		String measureName = st.nextToken();
		for(CubeMeasure measure: cubeDescriptor.getMeasures()) {
			if(measureName.equals(measure.getName())) {
				String family = OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX + measure.getName();
				columnName = measure.getName();
				if(logger.isDebugEnabled()) logger.debug(methodName + "family =  " + family + " columnName = "
						+ columnName);
				column = new Pair<byte[], byte[]>(Bytes.toBytes(family), Bytes.toBytes(columnName));
				break;
			}
		}
		if(column == null)
			throw new OLAPEngineException("Invalid measure in " + aggregate);
		reset();
	}
	
	public String getAggregateName() {
		return aggregateName;
	}

	public String getColumnName() {
		return columnName;
	}
	
	public Pair<byte[], byte[]> getColumn() {
		return column;
	}
	
	public int getColumnNuber() {
		return columnNumber;
	}
	
	public int getColumnNumber() {
		return columnNumber;
	}
	
	public abstract void reset();
	
	public abstract double getResult();
	
	public abstract void combine(double v);
	
	public abstract void reduce(double v);
}
