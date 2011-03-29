package org.olap4cloud.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class CubeScanTableInputFormat extends TableInputFormat {

	private CubeScanTableRecordReader cubeScanTableRecordReader = null;

	static Logger logger = Logger.getLogger(CubeScanTableInputFormat.class);

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		Pair<byte[][], byte[][]> keys = getHTable().getStartEndKeys();
		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
			throw new IOException("Expecting at least one region.");
		}
		if (getHTable() == null) {
			throw new IOException("No table was provided.");
		}
		CubeScan cubeScan = null;
		try {
			cubeScan = (CubeScan) DataUtils.stringToObject(context.getConfiguration().get(
					OLAPEngineConstants.JOB_CONF_PROP_CUBE_SCAN));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new IOException(e);
		}
		int count = 0;
		List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
		for (int i = 0; i < keys.getFirst().length; i++) {
			String regionLocation = getHTable().getRegionLocation(keys.getFirst()[i]).getServerAddress().getHostname();
			byte[] startRow = getScan().getStartRow();
			byte[] stopRow = getScan().getStopRow();
			if (!acceptRange(startRow, stopRow, cubeScan))
				continue;
			// determine if the given start an stop key fall into the region
			if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow,
					keys.getSecond()[i]) < 0)
					&& (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
				byte[] splitStart = startRow.length == 0 || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
						.getFirst()[i] : startRow;
				byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0)
						&& keys.getSecond()[i].length > 0 ? keys.getSecond()[i] : stopRow;
				splits.addAll(generateSplits(splitStart, splitStop, regionLocation, cubeScan));
			}
		}
		return splits;

	}

	private List<InputSplit> generateSplits(byte[] regionStart, byte[] regionStop, String regionLocation,
			CubeScan cubeScan) {
		List<InputSplit> r = new ArrayList<InputSplit>();
		List<Pair<byte[], byte[]>> splitRanges = new ArrayList<Pair<byte[], byte[]>>();
		for (Pair<byte[], byte[]> range : cubeScan.getRanges()) {
			if (DataUtils.compareRowKeys(regionStart, range.getSecond()) <= 0
					|| DataUtils.compareRowKeys(regionStop, range.getFirst()) >= 0) {
				byte splitStart[] = null;
				byte splitStop[] = null;
				if (DataUtils.compareRowKeys(regionStart, range.getFirst()) > 0)
					splitStart = regionStart;
				else
					splitStart = range.getFirst();
				if (DataUtils.compareRowKeys(regionStop, range.getSecond()) < 0)
					splitStop = regionStop;
				else
					splitStop = range.getSecond();
				splitRanges.add(new Pair<byte[], byte[]>(splitStart, splitStop));
			}
		}
		InputSplit split = new CubeScanTableSplit(getHTable().getTableName(), splitRanges, regionLocation);
		r.add(split);
		if (logger.isDebugEnabled())
			logger.debug("getSplits: split -> " + split);
		return r;
	}

	private boolean acceptRange(byte[] startRow, byte[] stopRow, CubeScan cubeScan) {
		String methodName = "acceptRange() ";
		for (Pair<byte[], byte[]> range : cubeScan.getRanges()) {
			if (logger.isDebugEnabled())
				logger.debug(methodName + "check startRow = " + LogUtils.describe(startRow) + ", stopRow = "
						+ LogUtils.describe(stopRow) + " and range = [" + LogUtils.describe(range.getFirst()) + ", "
						+ LogUtils.describe(range.getSecond()) + "]");
			if (DataUtils.compareRowKeys(startRow, range.getSecond()) <= 0
					&& DataUtils.compareRowKeys(stopRow, range.getFirst()) >= 0)
				return true;
		}
		if (logger.isDebugEnabled())
			logger.debug(methodName + "start and stop rows don't match any range.");
		return false;
	}

	@Override
	public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		CubeScanTableSplit tSplit = (CubeScanTableSplit) split;
		CubeScanTableRecordReader trr = this.cubeScanTableRecordReader;
		// if no table record reader was provided use default
		if (trr == null) {
			trr = new CubeScanTableRecordReader();
		}
		trr.setHTable(getHTable());
		try {
			trr.initialize(split, context);
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
			throw new IOException(e);
		}
		return trr;

	}

	protected class CubeScanTableRecordReader extends RecordReader<ImmutableBytesWritable, Result> {
		
		CubeScanTableSplit split = null;
		ImmutableBytesWritable key = new ImmutableBytesWritable();
		Result value = null;
		HTable table = null;
		ResultScanner scanner = null;
		Iterator<Pair<byte[], byte[]>> ranges = null;
		boolean finish = false;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			this.split = (CubeScanTableSplit)split;
			ranges = this.split.getSplitRanges().iterator();
			moveToNextRange();
		}
		
		void moveToNextRange() throws IOException {
			if(ranges.hasNext()) {
				Pair<byte[], byte[]> range = ranges.next();
				Scan scan = new Scan();
				scan.setStartRow(range.getFirst());
				scan.setStopRow(range.getSecond());
//				scan.setCaching(10000);
				close();
				scanner = table.getScanner(scan);
			} else
				finish = true;
		}
		
		@Override
		public void close() throws IOException {
			if(scanner != null)
				scanner.close();
		}

		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Result getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(finish)
				return false;
			value = scanner.next();
			while(!finish && value == null) {
				moveToNextRange();
				if(!finish)
					value = scanner.next();
			}
			if(finish)
				return false;
			key.set(value.getRow());
			return true;
		}
		
		public void setHTable(HTable table) {
			this.table = table;
		}
	}
}
