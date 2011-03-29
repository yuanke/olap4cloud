package org.olap4cloud.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.olap4cloud.test.DataImportHFileMR.DataImportMapper;

public class RangeAggregateMR {
	
	static Logger logger = Logger.getLogger(RangeAggregateMR.class);
	
	public static class RangeAggregateMapper extends TableMapper<LongWritable, LongWritable> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			long d1 = Bytes.toLong(value.getValue(Bytes.toBytes("data"), Bytes.toBytes("d1")));
			logger.debug("d1 = " + d1);
			context.write(new LongWritable(1), new LongWritable(d1));
		}
	}
	
	public static class RangeAggregateReducer extends Reducer<LongWritable, LongWritable
		, LongWritable, LongWritable> {
		@Override
		protected void reduce(LongWritable arg0,
				Iterable<LongWritable> it,
				org.apache.hadoop.mapreduce.Reducer<LongWritable, LongWritable
				, LongWritable, LongWritable>.Context c)
				throws IOException, InterruptedException {
			long s = 0;
			for(Iterator<LongWritable> i = it.iterator(); i.hasNext(); ) {
				LongWritable d = i.next();
				s ++;
			}
			c.write(new LongWritable(1), new LongWritable(s));
		}
	}
	
	public static void main(String argv[]) throws Exception {
		Job job = new Job();
		job.setJarByClass(RangeAggregateMR.class);
		Scan s = new Scan();
		s.setStartRow(new ImmutableBytesWritable(Bytes.toBytes(5)).get());
		s.setStopRow(new ImmutableBytesWritable(Bytes.toBytes(15)).get());
		TableMapReduceUtil.initTableMapperJob("testfacttable", s, RangeAggregateMapper.class
				, LongWritable.class, LongWritable.class, job);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(RangeAggregateReducer.class);
		FileOutputFormat.setOutputPath(job, new Path("/out"));
		job.waitForCompletion(true);
	}
}
