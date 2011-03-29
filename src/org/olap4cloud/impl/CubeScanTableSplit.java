package org.olap4cloud.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.olap4cloud.util.DataUtils;
import org.olap4cloud.util.LogUtils;

public class CubeScanTableSplit extends InputSplit implements Writable, Comparable<CubeScanTableSplit> {

	private byte[] tableName;
	private String regionLocation;
	List<Pair<byte[], byte[]>> splitRanges = null;

	public CubeScanTableSplit() {
		this(HConstants.EMPTY_BYTE_ARRAY, new ArrayList<Pair<byte[], byte[]>>(), "");
	}

	public CubeScanTableSplit(byte[] tableName, List<Pair<byte[], byte[]>> splitRanges, final String location) {
		this.tableName = tableName;
		this.regionLocation = location;
		this.splitRanges = splitRanges;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] { regionLocation };
	}

	public List<Pair<byte[], byte[]>> getSplitRanges() {
		return splitRanges;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		tableName = Bytes.readByteArray(in);
		regionLocation = Bytes.toString(Bytes.readByteArray(in));
		splitRanges = (List<Pair<byte[], byte[]>>)DataUtils.stringToObject(Bytes.toString(Bytes.readByteArray(in)));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, tableName);
		Bytes.writeByteArray(out, Bytes.toBytes(regionLocation));
		Bytes.writeByteArray(out, Bytes.toBytes(DataUtils.objectToString(splitRanges)));
	}

	@Override
	public String toString() {
		StringBuilder r = new StringBuilder();
		r.append("regionLocation = ").append(regionLocation).append(" ranges: ");
		for(Pair<byte[], byte[]> p: getSplitRanges()) {
			r.append("\n[").append(LogUtils.describe(p.getFirst())).append(" : ")
				.append(LogUtils.describe(p.getSecond())).append("]");
		}
		return r.toString();
	}

	@Override
	public int compareTo(CubeScanTableSplit split) {
		if(getSplitRanges().size() == 0 && split.getSplitRanges().size() == 0)
			return 0;
		if(getSplitRanges().size() == 0 && split.getSplitRanges().size() > 0)
			return -1;
		if(getSplitRanges().size() > 0 && split.getSplitRanges().size() == 0)
			return -1;
		return Bytes.compareTo(getSplitRanges().get(0).getFirst(), split.getSplitRanges().get(0).getFirst());
	}

}
