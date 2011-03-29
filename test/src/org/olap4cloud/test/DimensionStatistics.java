package org.olap4cloud.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DimensionStatistics {
	public static class RegionStaticticsMapper extends TableMapper<LongWritable, LongWritable> {
		@Override
		protected void map(ImmutableBytesWritable key, Result r,
				Context context) throws IOException, InterruptedException {
			Map<Long,Long> maxKeys = new HashMap<Long, Long>();
			Map<Long,Long> minKeys = new HashMap<Long, Long>();
			byte f[] = Bytes.toBytes("data");
			byte c[] = Bytes.toBytes("d1");
			long d1 = Bytes.toLong(r.getValue(f, c));
			long k = Bytes.toLong(key.get());
			
		}
	}
	
}
