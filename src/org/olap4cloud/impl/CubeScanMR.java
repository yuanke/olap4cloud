package org.olap4cloud.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeQueryResult;
import org.olap4cloud.client.CubeQueryResultRow;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class CubeScanMR {

	static Logger logger = Logger.getLogger(CubeScanMR.class);
	
	public static CubeQueryResult scan(CubeScan scan, CubeDescriptor cubeDescriptor) throws Exception {
		String methodName = "CubeScanMR.scan() ";
		Job job = new Job();
		job.setJarByClass(CubeScanMR.class);
		TableMapReduceUtil.initTableMapperJob(cubeDescriptor.getCubeDataTable(), scan.getHBaseScan()
				, CubeScanMRMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		job.setInputFormatClass(CubeScanTableInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(CubeScanMRReducer.class);
		job.setCombinerClass(CubeScanMRCombiner.class);
		String outPath = OLAPEngineConstants.MR_OUT_DIRECTORY_PREFIX + System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		job.getConfiguration().set(OLAPEngineConstants.JOB_CONF_PROP_CUBE_DESCRIPTOR
				, DataUtils.objectToString(cubeDescriptor));
		job.getConfiguration().set(OLAPEngineConstants.JOB_CONF_PROP_CUBE_SCAN
				, DataUtils.objectToString(scan));
		job.getConfiguration().setInt("mapred.map.tasks", 10000);
		job.waitForCompletion(true);
		return getCubeQueryResult(outPath, scan);
	}
	
	
	
	private static CubeQueryResult getCubeQueryResult(String outPath,
			CubeScan scan) throws Exception {
		CubeQueryResult cubeQueryResult = new CubeQueryResult();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path[] fileList = FileUtil.stat2Paths(fs.listStatus(new Path(outPath), new OutputLogFilter()));
		for(Path path: fileList) {
			FSDataInputStream fsin = fs.open(path);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsin));
			String s = null;
			do {
				s = reader.readLine();
				if(s != null) {
					StringTokenizer sb = new StringTokenizer(s, "\t", false);
					CubeQueryResultRow row = new CubeQueryResultRow();
					for(int i = 0; i < scan.getGroupBy().length; i ++)
						row.getGroupBy().add(Long.parseLong(sb.nextToken()));
					for(int i = 0; i < scan.getCubeScanAggregates().size(); i ++)
						row.getValues().add(Double.parseDouble(sb.nextToken()));
					cubeQueryResult.getRows().add(row);
				}
			} while(s != null);
			reader.close();
			fsin.close();
		}
		return cubeQueryResult;
	}



	public static class CubeScanMRMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>{
		
		CubeScan cubeScan = null;
		
		CubeScanFilter cubeScanFilter = null;
		
		byte outValues[];
		
		ImmutableBytesWritable outValuesWritable = new ImmutableBytesWritable();
		
		ImmutableBytesWritable outKeyWritable = new ImmutableBytesWritable();
		
		byte outKey[];
		
		int groupBy[];
		
		@Override
		protected void setup(Mapper<ImmutableBytesWritable,Result,ImmutableBytesWritable
				,ImmutableBytesWritable>.Context context) throws IOException ,InterruptedException {
			try {
				cubeScan = (CubeScan)DataUtils.stringToObject(context.getConfiguration()
						.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_SCAN));
				cubeScanFilter = new CubeScanFilter(cubeScan);
				outValues = new byte[cubeScan.getColumns().size() * 8];
				groupBy = cubeScan.getGroupBy();
				if(groupBy.length == 0) {
					outKey = new byte[1];
					outKey[0] = 0;
				} else 
					outKey = new byte[groupBy.length * 8];
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException(e.getMessage());
			}
		};
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			if(cubeScanFilter.filterRowKey(key.get(), 0, -1))
				return;
			byte inKey[] = key.get();
			String methodName = "CubeScanMRMapper.map() ";
			if(cubeScanFilter.filterRowKey(key.get(), 0, -1))
                return;
			if(logger.isDebugEnabled())
				logger.debug(methodName + "process key: " + LogUtils.describeKey(inKey));
			int n = cubeScan.getColumns().size();
			for(int i = 0; i < n; i ++) {
				Pair<byte[], byte[]> column = cubeScan.getColumns().get(i);
				byte val[] = value.getValue(column.getFirst(), column.getSecond());
				if(logger.isDebugEnabled()) logger.debug(methodName + " map measure: " + Bytes.toDouble(val));
				Bytes.putBytes(outValues, i * 8, val, 0, 8);
			}
			for(int i = 0; i < groupBy.length; i ++)
				Bytes.putBytes(outKey, i * 8, inKey, groupBy[i] * 8, 8);
			outKeyWritable.set(outKey);
			outValuesWritable.set(outValues);
			context.write(outKeyWritable, outValuesWritable); 
		}
	}
	
	public static class CubeScanMRCombiner extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable
	, ImmutableBytesWritable> {
		
		CubeScan cubeScan = null;
		
		byte outValues[];
		
		ImmutableBytesWritable outValuesWritable = new ImmutableBytesWritable();
		
		LongWritable outKey = new LongWritable(1);
		
		List<CubeScanAggregate> aggregates = null;
		
		double inValues[];
		
		int inN = 0;
		
		int outN = 0;
		
		protected void setup(org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable,ImmutableBytesWritable
				,ImmutableBytesWritable,ImmutableBytesWritable>.Context context) throws IOException ,InterruptedException {
			try {
				cubeScan = (CubeScan)DataUtils.stringToObject(context.getConfiguration()
						.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_SCAN));
				outValues = new byte[cubeScan.getCubeScanAggregates().size() * 8];
				inValues = new double[cubeScan.getColumns().size()];
				inN = inValues.length;
				aggregates = cubeScan.getCubeScanAggregates();
				outN = aggregates.size();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException(e.getMessage());
			}			
		};
				
		@Override
		protected void reduce(ImmutableBytesWritable inKey,
				Iterable<ImmutableBytesWritable> inValIter,
				org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable, ImmutableBytesWritable
				, ImmutableBytesWritable
				, ImmutableBytesWritable>.Context context)
				throws IOException, InterruptedException {
			String methodName = "CubeScanMRCombiner.reduce() ";
			if(logger.isDebugEnabled()) logger.debug(methodName + "reduce key: " + inKey.get());
			for(CubeScanAggregate aggregate: aggregates)
				aggregate.reset();
			for(ImmutableBytesWritable inVal: inValIter) {
				byte buf[] = inVal.get();
				for(int i = 0; i < inN; i ++)
					inValues[i] = Bytes.toDouble(buf, i * 8);
				for(CubeScanAggregate aggregate: aggregates)
					aggregate.combine(inValues[aggregate.getColumnNuber()]);
				if(logger.isDebugEnabled()) logger.debug(methodName + " combined values: " + LogUtils.describe(inValues));
			}
			for(int i = 0; i < outN; i ++)
				Bytes.putDouble(outValues, i * 8, aggregates.get(i).getResult());
			outValuesWritable.set(outValues);
			context.write(inKey, outValuesWritable);
		}
		
	}
	
	public static class CubeScanMRReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable
		, Text, Text> {
		
		CubeScan cubeScan = null;
		
		List<CubeScanAggregate> aggregates;
		
		double inValues[];
		
		int inN;
		
		Text outVal = new Text();
		
		Text outKey = new Text();
		
		StringBuilder outValSB = new StringBuilder();
		
		StringBuilder outKeySB = new StringBuilder();
		
		int groupBy[];

		protected void setup(org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable,ImmutableBytesWritable
				,Text,Text>
			.Context context) throws IOException ,InterruptedException {
			try {
				cubeScan = (CubeScan)DataUtils.stringToObject(context.getConfiguration()
					.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_SCAN));
				aggregates = cubeScan.getCubeScanAggregates();
				inN = aggregates.size();
				inValues = new double[inN];
				groupBy = cubeScan.getGroupBy();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException(e.getMessage());
			}
		};
		
		@Override
		protected void reduce(ImmutableBytesWritable inKey,
				Iterable<ImmutableBytesWritable> inVals,
				org.apache.hadoop.mapreduce.Reducer<ImmutableBytesWritable, ImmutableBytesWritable
				, Text
				, Text>.Context context)
				throws IOException, InterruptedException {
			String methodName = "CubeScanMRReducer.reduce() ";
			for(CubeScanAggregate aggregate: aggregates)
				aggregate.reset();
			for(ImmutableBytesWritable inWritable: inVals) {
				byte buf[] = inWritable.get();
				for(int i = 0; i < inN; i ++)
					aggregates.get(i).reduce(Bytes.toDouble(buf, i * 8));
			}
			outKeySB.setLength(0);
			byte inKeyVal[] = inKey.get();
			for(int i = 0; i < groupBy.length; i ++)
				outKeySB.append(Bytes.toLong(inKeyVal, i * 8)).append("\t");
			outValSB.setLength(0);
			for(CubeScanAggregate aggregate: aggregates)
				outValSB.append(aggregate.getResult() + "\t");
			outKey.set(outKeySB.toString());
			outVal.set(outValSB.toString());
			context.write(outKey, outVal);
		}
		
	}
}
