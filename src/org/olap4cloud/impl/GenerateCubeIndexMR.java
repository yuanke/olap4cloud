package org.olap4cloud.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.OLAPEngineException;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class GenerateCubeIndexMR {
	static Logger logger = Logger.getLogger(GenerateCubeIndexMR.class);
	
	public static class GenerateCubeIndexMapper extends TableMapper<ImmutableBytesWritable, CubeIndexEntry> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			String methodName = "GenerateCubeIndexMapper.map() ";
			byte keyBytes[] = key.get();
			if(logger.isDebugEnabled()) logger.debug(methodName + "generate index for data row with key =  " 
					+ LogUtils.describe(keyBytes));
			int len = (keyBytes.length) / 8;
			for(int i = 0; i < len; i ++) {
				int dimNumber = i;
				byte keyData[] = Arrays.copyOfRange(keyBytes, i * 8, (i + 1) * 8);
				byte outKey[] = DataUtils.pack(dimNumber, keyData);
				int indexLength = (i + 1) * 8;
				byte indexData[] = Arrays.copyOfRange(keyBytes, 0, (i + 1) * 8);
				if(logger.isDebugEnabled()) logger.debug(methodName + "map index for key: " 
						+ LogUtils.describe(outKey));
				context.write(new ImmutableBytesWritable(outKey), new CubeIndexEntry(indexLength, indexData));
			} 
		}
	}
	
	public static class GenerateCubeIndexReducer 
		extends TableReducer<ImmutableBytesWritable, CubeIndexEntry, ImmutableBytesWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable inKey,
				Iterable<CubeIndexEntry> vals,
				Context context)
				throws IOException, InterruptedException {
			String methodName = "GenerateCubeIndexReducer.reduce() ";
			Set<CubeIndexEntry> index = new TreeSet<CubeIndexEntry>(new CubeIndexEntryComparator());
			for(Iterator<CubeIndexEntry> i = vals.iterator(); i.hasNext(); ) { 
				CubeIndexEntry e = i.next();
				index.add(new CubeIndexEntry(e.getLength(), e.getData()));
				if(logger.isDebugEnabled())
					logger.debug(methodName + "added entry to index for key " + LogUtils.describe(inKey.get()) 
							+ " index size is " + index.size() + " indexentry: " + LogUtils.describe(e));
				if(index.size() > 10000)
					index = reduceIndex(index);
			}
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(bout);
			for(Iterator<CubeIndexEntry> i = index.iterator(); i.hasNext();)
				i.next().write(dout);
			dout.close();
			bout.close();
			byte indexData[] = bout.toByteArray();
			if(logger.isDebugEnabled()) logger.debug(methodName + "generate index entry with key = " 
					+ LogUtils.describe(inKey.get()) + " size of index is: " + index.size());
			Put put = new Put(inKey.get());
			put.add(Bytes.toBytes(OLAPEngineConstants.CUBE_INDEX_COLUMN), Bytes.toBytes(OLAPEngineConstants.CUBE_INDEX_COLUMN),
					indexData);
			context.write(inKey, put);
		}

		private Set<CubeIndexEntry> reduceIndex(Set<CubeIndexEntry> index) {
			while(index.size() > 9000) {
				int maxLen = 0;
				for(CubeIndexEntry e: index) 
					if(maxLen < e.getLength())
						maxLen = e.getLength();
				if(maxLen == 0)
					return new TreeSet<CubeIndexEntry>();
				Set<CubeIndexEntry> r = new TreeSet<CubeIndexEntry>();
				for(CubeIndexEntry e: index) {
					if(e.getLength() == maxLen)
						e.setLength(e.getLength() - 1);
					r.add(e);
				}
				index = r;
			}
			return index;
		}
	}
	
	public static void generate(CubeDescriptor descr) throws OLAPEngineException {
		try {
			HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());
			HTableDescriptor tableDescr = new HTableDescriptor(descr
					.getCubeIndexTable());
			tableDescr.addFamily(new HColumnDescriptor(
					OLAPEngineConstants.CUBE_INDEX_COLUMN));
			if (admin.tableExists(descr.getCubeIndexTable())) {
				admin.disableTable(descr.getCubeIndexTable());
				admin.deleteTable(descr.getCubeIndexTable());
			}
			admin.createTable(tableDescr);
			Job job = new Job();
			job.setJarByClass(GenerateCubeIndexMR.class);
			Scan scan = new Scan();
			scan.setCaching(1000);
			TableMapReduceUtil.initTableMapperJob(descr.getCubeDataTable(),
					scan, GenerateCubeIndexMapper.class,
					ImmutableBytesWritable.class, CubeIndexEntry.class, job);
			TableMapReduceUtil.initTableReducerJob(descr.getCubeIndexTable(),
					GenerateCubeIndexReducer.class, job);
			job.waitForCompletion(true);
		} catch (Exception e) {
			throw new OLAPEngineException(e);
		}
	}
}
