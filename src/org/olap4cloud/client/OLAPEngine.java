package org.olap4cloud.client;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.log4j.Logger;
import org.olap4cloud.impl.AggregationCubeDescriptor;
import org.olap4cloud.impl.CubeIndexEntry;
import org.olap4cloud.impl.CubeScan;
import org.olap4cloud.impl.CubeScanAggregate;
import org.olap4cloud.impl.CubeScanCondition;
import org.olap4cloud.impl.CubeScanMR;
import org.olap4cloud.impl.GenerateAggregationCubeMR;
import org.olap4cloud.impl.GenerateCubeIndexMR;
import org.olap4cloud.impl.GenerateCubeMR;
import org.olap4cloud.impl.OLAPEngineConstants;
import org.olap4cloud.impl.aggr.CountCubeScanAggregate;
import org.olap4cloud.impl.aggr.MaxCubeScanAggregate;
import org.olap4cloud.impl.aggr.MinCubeScanAggregate;
import org.olap4cloud.impl.aggr.SumCubeScanAggregate;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;


public class OLAPEngine {
	
	static Logger logger = Logger.getLogger(OLAPEngine.class);
	
	HBaseConfiguration config = new HBaseConfiguration();
	
	public OLAPEngine() {
		
	}
	
	public CubeQueryResult executeQuery(CubeQuery query, CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		String methodName = "executeQuery() ";
		try {
			if(logger.isDebugEnabled()) logger.debug(methodName + "cubeDescriptor = " 
					+ cubeDescriptor.toString());
			List<String> usedDimensions = getUsedDimensions(query);
			AggregationCubeDescriptor aggCube = getBestAggregationCube(usedDimensions, cubeDescriptor);
			if(aggCube == null) {
				if(logger.isDebugEnabled()) logger.debug(methodName + "can't find aggregation cube. " +
						"Use data cube instead");
				CubeScan scan = getCubeScan(query, cubeDescriptor);
				return CubeScanMR.scan(scan, cubeDescriptor);
			} else {
				if(logger.isDebugEnabled()) logger.debug(methodName + " use aggregation cube " 
						+ aggCube.getCubeName());
				aggregateMeasures(query);
				CubeScan scan = getCubeScan(query, aggCube);
				return CubeScanMR.scan(scan, aggCube);
			}
		} catch(Exception e) {
			throw new OLAPEngineException(e);
		}
	}
	
	private void aggregateMeasures(CubeQuery query) {
		for(CubeQueryAggregate aggr: query.getAggregates()) {
			StringTokenizer st = new StringTokenizer(aggr.getAggregate(), "()", false);
			String op = st.nextToken();
			String op2 = op;
			if(op.equalsIgnoreCase("count"))
				op = "sum";
			String name = st.nextToken();
			String newAggregate = op + "(" + name + "_" + op2 + ")";
			logger.debug("change aggregate to " + newAggregate);
			aggr.setAggregate(newAggregate);
		}
	}

	private AggregationCubeDescriptor getBestAggregationCube(
			List<String> usedDimensions, CubeDescriptor dataCube) throws Exception {
		AggregationCubeDescriptor bestAggCube = null;
		for(AggregationCubeDescriptor aggCube: dataCube.getAggregationCubes()) {
			boolean dimNotFound = false;
			for(String dimName: usedDimensions) 
				if(!aggCube.containsDimensions(dimName)) {
					dimNotFound = true;
					break;
				}
			if(!dimNotFound && (bestAggCube == null || bestAggCube.getDimensions().size() 
					> aggCube.getDimensions().size())) 
				bestAggCube = aggCube;
		}
		return bestAggCube;
	}

	private List<String> getUsedDimensions(CubeQuery query) {
		Set<String> dims = new HashSet<String>();
		dims.addAll(query.getGroupBy());
		for(CubeQueryCondition condition: query.getConditions()) 
			dims.add(condition.getDimensionName());
		List<String> ret = new ArrayList<String>();
		ret.addAll(dims);
		return ret;
	}

	private CubeScan getCubeScan(CubeQuery query, CubeDescriptor cubeDescriptor) throws Exception{
		String methodName  = "getCubeScan() ";
		CubeScan scan = new CubeScan();
		List<CubeIndexEntry> index = null;
		for(CubeQueryCondition condition: query.getConditions()) {
			String dimensionName = condition.getDimensionName();
			if(logger.isDebugEnabled()) logger.debug(methodName + "process dimension " + dimensionName);
			int dimensionNumber = getDimensionNumber(dimensionName, cubeDescriptor);
			if(logger.isDebugEnabled()) logger.debug(methodName + "dimensionNumber = " + dimensionNumber);
			List<CubeIndexEntry> dimIndex = new ArrayList<CubeIndexEntry>();
			for(long dimVal: condition.dimensionValues) {
				List<CubeIndexEntry> dimValIndex = getIndexForDimensionValue(dimensionNumber, dimVal, cubeDescriptor);
				dimIndex = mergeIndexes(dimIndex, dimValIndex);
			}
			if(index == null)
				index = dimIndex;
			else
				index = joinIndexes(index, dimIndex);
			long values[] = new long[condition.getDimensionValues().size()];
			for(int i = 0; i < values.length; i ++)
				values[i] = condition.getDimensionValues().get(i);
			scan.getConditions().add(new CubeScanCondition(dimensionNumber, values));
		}
		if(logger.isDebugEnabled()) logger.debug(methodName + "final index size: " + index.size());
		for(CubeIndexEntry indexEntry: index) {
			byte startRow[] = getStartRow(indexEntry, cubeDescriptor.dimensions.size());
			byte stopRow[] = getStopRow(indexEntry, cubeDescriptor.dimensions.size());
			if(logger.isDebugEnabled()) logger.debug(methodName + "add range [" + LogUtils.describe(startRow)
					+ ", " + LogUtils.describe(stopRow) + "]  to scan.");
			Pair<byte[], byte[]> range = new Pair<byte[], byte[]>(startRow, stopRow);
			scan.getRanges().add(range);
		}
		for(CubeQueryAggregate aggregate: query.getAggregates()) 
			scan.getCubeScanAggregates().add(getCubeScanAggregate(aggregate, cubeDescriptor));
		int groupBy[] = new int[query.getGroupBy().size()];
		for(int i = 0; i < groupBy.length; i ++)
			groupBy[i] = getDimensionNumber(query.getGroupBy().get(i), cubeDescriptor);
		scan.setGroupBy(groupBy);
		scan.prepare();
		return scan;
	}

	private CubeScanAggregate getCubeScanAggregate(CubeQueryAggregate aggregate, CubeDescriptor cubeDescriptor) 
		throws OLAPEngineException {
		if(aggregate.getAggregate().toLowerCase().startsWith("sum("))
			return new SumCubeScanAggregate(aggregate.getAggregate(), cubeDescriptor);
		if(aggregate.getAggregate().toLowerCase().startsWith("max("))
			return new MaxCubeScanAggregate(aggregate.getAggregate(), cubeDescriptor);
		if(aggregate.getAggregate().toLowerCase().startsWith("min("))
			return new MinCubeScanAggregate(aggregate.getAggregate(), cubeDescriptor);
		if(aggregate.getAggregate().toLowerCase().startsWith("count("))
			return new CountCubeScanAggregate(aggregate.getAggregate(), cubeDescriptor);
		throw new OLAPEngineException("can't process aggregate " + aggregate.getAggregate());
	}

	private byte[] getStopRow(CubeIndexEntry index, int size) {
		int rSize = (size + 1) * 8;
		byte r[] = new byte[rSize];
		Bytes.putBytes(r, 0, index.getData(), 0, index.getLength());
		for(int i = index.getLength(); i < rSize; i ++) 
			r[i] = Byte.MIN_VALUE;
		return r;
	}

	private byte[] getStartRow(CubeIndexEntry index, int size) {
		int rSize = (size + 1) * 8;
		byte r[] = new byte[rSize];
		Bytes.putBytes(r, 0, index.getData(), 0, index.getLength());
		for(int i = index.getLength(); i < rSize; i ++) 
			r[i] = 0;
		return r;
	}

	private List<CubeIndexEntry> joinIndexes(List<CubeIndexEntry> i1,
			List<CubeIndexEntry> i2) {
		List<CubeIndexEntry> r = new ArrayList<CubeIndexEntry>();
		for(CubeIndexEntry e1: i1) 
			for(CubeIndexEntry e2: i2) {
				boolean c1 = e1.contain(e2);
				boolean c2 = e2.contain(e1);
				if(c1 && c2)
					r.add(e1);
				if(c1 && !c2)
					r.add(e2);
				if(c2 && !c1)
					r.add(e1);
			}
		return r;
	}

	private List<CubeIndexEntry> mergeIndexes(List<CubeIndexEntry> i1,
			List<CubeIndexEntry> i2) {
		List<CubeIndexEntry> r1 = new ArrayList<CubeIndexEntry>();
		for(CubeIndexEntry e1: i1) {
			boolean contained = false;
			for(CubeIndexEntry e2: i2) 
				if(e2.contain(e1)) {
					contained = true;
					break;
				}
			if(!contained)
				r1.add(e1);
		}
		List<CubeIndexEntry> r2 = new ArrayList<CubeIndexEntry>();
		for(CubeIndexEntry e1: i2) {
			boolean contained = false;
			for(CubeIndexEntry e2: r1) 
				if(e2.contain(e1)) {
					contained = true;
					break;
				}
			if(!contained)
				r2.add(e1);
		}
		r1.addAll(r2);
		return r1;
	}

	private List<CubeIndexEntry> getIndexForDimensionValue(int dimensionNumber,
			long dimVal, CubeDescriptor cubeDescriptor) throws Exception {
		String methodName = "getIndexForDimensionValue() ";
		if(logger.isDebugEnabled()) logger.debug(methodName + "dimensionNumber = " + 
				dimensionNumber + " dimVal = " + dimVal);
		HTable hTable = new HTable(cubeDescriptor.getCubeIndexTable());
		byte key[] = DataUtils.pack(dimensionNumber, dimVal);
		if(logger.isDebugEnabled()) logger.debug(methodName + "index key = " + LogUtils.describe(key));
		Get get = new Get(key);
		byte indexColumn[] = Bytes.toBytes(OLAPEngineConstants.CUBE_INDEX_COLUMN);
		get.addColumn(indexColumn, indexColumn);
		byte index[] = hTable.get(get).getValue(indexColumn, indexColumn);
		if(logger.isDebugEnabled()) logger.debug(methodName + "index = " + LogUtils.describe(index));
		if(index == null)
			return new ArrayList<CubeIndexEntry>();
		DataInputBuffer buf = new DataInputBuffer();
		buf.reset(index, index.length);
		List<CubeIndexEntry> result = new ArrayList<CubeIndexEntry>();
		try {
			while(true) {
				CubeIndexEntry entry = new CubeIndexEntry();
				entry.readFields(buf);
				result.add(entry);
			}
		} catch(EOFException e) {
		}
		return result;
	}

	private int getDimensionNumber(String dimensionName,
			CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		for(int i = 0; i < cubeDescriptor.dimensions.size(); i ++) {
			if(cubeDescriptor.dimensions.get(i).getName().equals(dimensionName))
				return i;
		}
		throw new OLAPEngineException("Can't find dimension " + dimensionName);
	}
	
	public void generateCube(CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		GenerateCubeMR.generateCube(cubeDescriptor);
		GenerateCubeIndexMR.generate(cubeDescriptor);
		for(AggregationCubeDescriptor aggCube: cubeDescriptor.getAggregationCubes()) {
			GenerateAggregationCubeMR.generateCube(aggCube, cubeDescriptor);
			GenerateCubeIndexMR.generate(aggCube);
		}
	}
}
