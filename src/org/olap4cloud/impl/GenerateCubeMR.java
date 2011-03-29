package org.olap4cloud.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.OLAPEngineException;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class GenerateCubeMR {
	
	public static class GenerateCubeMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		
		static Logger logger = Logger.getLogger(GenerateCubeMapper.class);
		
		CubeDescriptor cubeDescriptor;
		
		int dimensionN;
		
		int measureN;
		
		byte measureNames[][];
		
		byte measureFamilies[][];
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			String methodName = "GenerateCubeMapper.setup() ";
			try {
				cubeDescriptor = (CubeDescriptor)DataUtils.stringToObject(context.getConfiguration()
					.get(OLAPEngineConstants.JOB_CONF_PROP_CUBE_DESCRIPTOR));
				dimensionN = cubeDescriptor.getDimensions().size();
				measureN = cubeDescriptor.getMeasures().size();
				if(logger.isDebugEnabled()) logger.debug(methodName + "retrieved measureN = " + measureN);
				measureNames = new byte[measureN][];
				measureFamilies = new byte[measureN][];
				for(int i = 0; i < measureN; i ++) {
					measureNames[i] = Bytes.toBytes(cubeDescriptor.getMeasures().get(i).getName());
					measureFamilies[i] = Bytes.toBytes(OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX 
							+ cubeDescriptor.getMeasures().get(i).getName());
				}
			} catch(Exception e) {
				logger.error(e.getMessage(), e);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			String methodName = "GenerateCubeMapper.map() ";
			long dimensions[] = new long[dimensionN + 1];
			String tokens[] = value.toString().split("\t");
			for(int i = 0; i < dimensionN; i ++)
				dimensions[i] = Long.parseLong(tokens[i]);
			dimensions[dimensions.length - 1] = key.get();
			byte cubeKey[] = DataUtils.pack(dimensions);
			Put put = new Put(cubeKey);
			for(int i = 0; i < measureN; i ++) {		
				byte putValue[] = Bytes.toBytes(Double.parseDouble(tokens[dimensionN + i]));
				put.add(measureFamilies[i], measureNames[i], 
						putValue);
				if(logger.isDebugEnabled()) logger.debug(methodName + " key = " + LogUtils.describeKey(cubeKey) 
						+ " added value: " + LogUtils.describe(putValue)
						+ " to column: " + LogUtils.describe(measureFamilies[i]) + " : "
						+ LogUtils.describe(measureNames[i]));
			}
			context.write(new ImmutableBytesWritable(cubeKey), put);
		}
	}
	
	public static void generateCube(CubeDescriptor cubeDescriptor) throws OLAPEngineException {
		try {
			HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
			HTableDescriptor tableDescr = new HTableDescriptor(cubeDescriptor
					.getCubeDataTable());
			for (int i = 0; i < cubeDescriptor.getMeasures().size(); i++)
				tableDescr
						.addFamily(new HColumnDescriptor(
								OLAPEngineConstants.DATA_CUBE_MEASURE_FAMILY_PREFIX
										+ cubeDescriptor.getMeasures().get(i)
												.getName()));
			if (admin.tableExists(cubeDescriptor.getCubeDataTable())) {
				admin.disableTable(cubeDescriptor.getCubeDataTable());
				admin.deleteTable(cubeDescriptor.getCubeDataTable());
			}
			admin.createTable(tableDescr);
			Job job = new Job();
			job.setJarByClass(GenerateCubeMR.class);
			job.setMapperClass(GenerateCubeMapper.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			FileInputFormat.setInputPaths(job, new Path(cubeDescriptor
					.getSourceDataDir()));
			TableMapReduceUtil.initTableReducerJob(cubeDescriptor
					.getCubeDataTable(), IdentityTableReducer.class, job);
			job.getConfiguration().set(
					OLAPEngineConstants.JOB_CONF_PROP_CUBE_DESCRIPTOR,
					DataUtils.objectToString(cubeDescriptor));
			job.waitForCompletion(true);
		} catch (Exception e) {
			throw new OLAPEngineException(e);
		}
	}
}
