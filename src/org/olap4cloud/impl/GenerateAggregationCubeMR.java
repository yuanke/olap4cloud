package org.olap4cloud.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeDimension;
import org.olap4cloud.client.OLAPEngineException;
import org.olap4cloud.util.DataUtils;

public class GenerateAggregationCubeMR {

	static Logger logger = Logger.getLogger(GenerateAggregationCubeMR.class);
	
	public static void generateCube(AggregationCubeDescriptor aggCube,
			CubeDescriptor dataCube) throws OLAPEngineException {
		try {
			createTable(aggCube);
			Job job = new Job();
			job.setJarByClass(GenerateAggregationCubeMR.class);
			Scan scan = new Scan();
			scan.setCaching(1000);
			TableMapReduceUtil.initTableMapperJob(dataCube.getCubeDataTable(),
					scan, GenerateAggregationCubeMapper.class,
					ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
			TableMapReduceUtil.initTableReducerJob(aggCube.getCubeDataTable()
					, GenerateAggregationCubeReducer.class, job);
			job.setCombinerClass(GenerateAggregationCubeCombiner.class);
			
			job.getConfiguration().set(OLAPEngineConstants.JOB_CONF_PROP_DATA_CUBE_DESCRIPTOR, 
					DataUtils.objectToString(dataCube));
			job.getConfiguration().set(OLAPEngineConstants.JOB_CONF_PROP_AGG_CUBE_DESCRIPTOR, 
					DataUtils.objectToString(aggCube));
			job.waitForCompletion(true);
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new OLAPEngineException(e);
		}
	}

	private static void createTable(AggregationCubeDescriptor cubeDescriptor) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor(cubeDescriptor.getCubeDataTable());
		for (int i = 0; i < cubeDescriptor.getMeasures().size(); i++)
			tableDescr.addFamily(new HColumnDescriptor(	OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX
									+ cubeDescriptor.getMeasures().get(i)
											.getName()));
		if (admin.tableExists(cubeDescriptor.getCubeDataTable())) {
			admin.disableTable(cubeDescriptor.getCubeDataTable());
			admin.deleteTable(cubeDescriptor.getCubeDataTable());
		}
		admin.createTable(tableDescr);
	}

	public static class GenerateAggregationCubeMapper extends TableMapper<ImmutableBytesWritable
			, ImmutableBytesWritable> {
		
		static Logger logger = Logger.getLogger(GenerateAggregationCubeMapper.class);
		
		int aggDimensionIndexes[];
		
		int keyOutN;
		
		int valueOutN;
		
		byte outKey[];
		
		byte outValue[];
		
		ImmutableBytesWritable outKeyWritable = new ImmutableBytesWritable();
		
		ImmutableBytesWritable outValueWritable = new ImmutableBytesWritable();
		
		Pair<byte[], byte[]> aggColumns[];
		
		protected void setup(Mapper<ImmutableBytesWritable,Result,ImmutableBytesWritable,ImmutableBytesWritable>
				.Context context) 
					throws java.io.IOException ,InterruptedException {
			try {
				CubeDescriptor dataCube = (CubeDescriptor)DataUtils.stringToObject(context.getConfiguration()
					.get(OLAPEngineConstants.JOB_CONF_PROP_DATA_CUBE_DESCRIPTOR));
				AggregationCubeDescriptor aggCube = (AggregationCubeDescriptor)DataUtils
						.stringToObject(context.getConfiguration()
						.get(OLAPEngineConstants.JOB_CONF_PROP_AGG_CUBE_DESCRIPTOR));
				aggDimensionIndexes = new int[aggCube.getDimensions().size()];
				for(int i = 0; i < aggDimensionIndexes.length; i ++) {
					CubeDimension aggDimension = aggCube.getDimensions().get(i);
					for(int j = 0; j < dataCube.getDimensions().size(); j ++) {
						CubeDimension dataDimension = dataCube.getDimensions().get(j);
						if(aggDimension.getName().equals(dataDimension.getName())) {
							aggDimensionIndexes[i] = j;
						}
					}
				}
				keyOutN = aggDimensionIndexes.length;
				outKey = new byte[(keyOutN + 1) * 8];
				valueOutN = aggCube.getAggregates().size();
				outValue = new byte[valueOutN * 8];
				aggColumns = new Pair[valueOutN];
				for(int i = 0; i < valueOutN; i ++) {
					String columnName = aggCube.getAggregates().get(i).getColumnName();
					aggColumns[i] = new Pair<byte[], byte[]>(
							Bytes.toBytes(OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX + columnName)
							, Bytes.toBytes(columnName));
				}
			} catch(Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException(e.getMessage());
			}
		};
		
		protected void map(ImmutableBytesWritable keyWritable, Result value, 
				Mapper<ImmutableBytesWritable,Result,ImmutableBytesWritable,ImmutableBytesWritable>.Context context) 
		throws java.io.IOException ,InterruptedException {
			byte key[] = keyWritable.get();
			for(int i = 0; i < keyOutN; i ++)
				Bytes.putBytes(outKey, i * 8, key, aggDimensionIndexes[i] * 8, 8);
			Bytes.putLong(outKey, keyOutN * 8, 0);
			outKeyWritable.set(outKey);
			for(int i = 0; i < valueOutN; i ++)
				Bytes.putBytes(outValue, i * 8, value.getValue(aggColumns[i].getFirst()
						, aggColumns[i].getSecond()), 0, 8);
			outValueWritable.set(outValue);
			context.write(outKeyWritable, outValueWritable);
		};
	}
	
	public static class GenerateAggregationCubeCombiner extends Reducer<ImmutableBytesWritable
	, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		static Logger logger = Logger.getLogger(GenerateAggregationCubeCombiner.class);
		
		AggregationCubeDescriptor aggCube;
		
		String sColumns[];
		
		byte outValue[];
		
		int outN;
		
		ImmutableBytesWritable outValueWritable = new ImmutableBytesWritable();
		
		protected void setup(Reducer<ImmutableBytesWritable,ImmutableBytesWritable,ImmutableBytesWritable
				,ImmutableBytesWritable>.Context context) 
				throws java.io.IOException ,InterruptedException {
			try {
				aggCube = (AggregationCubeDescriptor)DataUtils.stringToObject(context.getConfiguration()
						.get(OLAPEngineConstants.JOB_CONF_PROP_AGG_CUBE_DESCRIPTOR));
				CubeDescriptor dataCube = (CubeDescriptor)DataUtils.stringToObject(context.getConfiguration()
						.get(OLAPEngineConstants.JOB_CONF_PROP_DATA_CUBE_DESCRIPTOR));
				outN = aggCube.getAggregates().size();
				outValue = new byte[outN * 8];
			} catch(Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException();
			}
		};
		
		protected void reduce(ImmutableBytesWritable inKey, Iterable<ImmutableBytesWritable> values
				, Reducer<ImmutableBytesWritable,Result,ImmutableBytesWritable,ImmutableBytesWritable>
				.Context context) throws java.io.IOException ,InterruptedException {
			for(CubeScanAggregate aggregate: aggCube.getAggregates())
				aggregate.reset();
			for(Iterator<ImmutableBytesWritable> iterator = values.iterator(); iterator.hasNext(); ) {
				byte value[] = iterator.next().get();
				for(int i = 0; i < outN; i ++)
					aggCube.getAggregates().get(i).combine(Bytes.toDouble(value, i * 8));
			}
			for(int i = 0; i < outN; i ++)
				Bytes.putDouble(outValue, i * 8, aggCube.getAggregates().get(i).getResult());
			outValueWritable.set(outValue);
			context.write(inKey, outValueWritable);
		};
	}
	
	public static class GenerateAggregationCubeReducer	extends	TableReducer<ImmutableBytesWritable
			, ImmutableBytesWritable, ImmutableBytesWritable> {

		static Logger logger = Logger.getLogger(GenerateAggregationCubeReducer.class);

		AggregationCubeDescriptor aggCube;

		Pair<byte[], byte[]> columns[];

		int inN;

		protected void setup(Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable
				, Writable>.Context context)
				throws java.io.IOException, InterruptedException {
			try {
				aggCube = (AggregationCubeDescriptor)DataUtils.stringToObject(context.getConfiguration()
								.get(OLAPEngineConstants.JOB_CONF_PROP_AGG_CUBE_DESCRIPTOR));
				columns = new Pair[aggCube.getAggregates().size()];
				for (int i = 0; i < columns.length; i++) {
					String measureName = aggCube.getMeasures().get(i).getName();
					columns[i] = new Pair<byte[], byte[]>(
							Bytes.toBytes(OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX
											+ measureName), Bytes.toBytes(measureName));
				}
				inN = columns.length;
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException();
			}
		};

		protected void reduce(ImmutableBytesWritable inKey,Iterable<ImmutableBytesWritable> inValues,
				Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable
				, Writable>.Context context) throws java.io.IOException, InterruptedException {
			for (CubeScanAggregate aggregate : aggCube.getAggregates())
				aggregate.reset();
			for (Iterator<ImmutableBytesWritable> iterator = inValues
					.iterator(); iterator.hasNext();) {
				byte inValue[] = iterator.next().get();
				for (int i = 0; i < inN; i++) {
					double value = Bytes.toDouble(inValue, i * 8);
					aggCube.getAggregates().get(i).reduce(value);
				}
			}
			Put put = new Put(inKey.get());
			for(int i = 0; i < inN; i ++) {
				double value = aggCube.getAggregates().get(i).getResult();
				put.add(columns[i].getFirst(), columns[i].getSecond(), Bytes.toBytes(value));
			}
			context.write(inKey, put);
		};

	}

}
