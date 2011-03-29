package org.olap4cloud.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class DataImportHFileMR {
	
	static Logger logger = Logger.getLogger(DataImportHFileMR.class);
	
	static class DataImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		@Override
		protected void map(LongWritable key, Text value
				, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			String s = value.toString();
			StringTokenizer st1 = new StringTokenizer(s, "\n", false);
			while(st1.hasMoreTokens()) {
				StringTokenizer st2 = new StringTokenizer(st1.nextToken(), "\t", false);
				long k = Long.parseLong(st2.nextToken());
				Put put = new Put(Bytes.toBytes(k));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("d1")
						, Bytes.toBytes(Long.parseLong(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("d2")
						, Bytes.toBytes(Long.parseLong(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("d3")
						, Bytes.toBytes(Long.parseLong(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("m1")
						, Bytes.toBytes(Double.parseDouble(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("m2")
						, Bytes.toBytes(Double.parseDouble(st2.nextToken())));
				put.add(Bytes.toBytes("data"), Bytes.toBytes("m3")
						, Bytes.toBytes(Double.parseDouble(st2.nextToken())));
				context.write(new ImmutableBytesWritable(Bytes.toBytes(k)), put);
			}
		}
	}
	
	public static void main(String args[]) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescr = new HTableDescriptor("testfacttable");
		tableDescr.addFamily(new HColumnDescriptor("data"));
		if(admin.tableExists("testfacttable")) {
			admin.disableTable("testfacttable");
			admin.deleteTable("testfacttable");
		}
		admin.createTable(tableDescr);
		Job job = new Job();
		job.setJarByClass(DataImportHFileMR.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/data/data.txt"));
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setMapperClass(DataImportMapper.class);
		TableMapReduceUtil.initTableReducerJob("testfacttable", IdentityTableReducer.class, job);
		job.waitForCompletion(true);
	}
}
