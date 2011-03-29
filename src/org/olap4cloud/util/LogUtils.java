package org.olap4cloud.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.olap4cloud.impl.CubeIndexEntry;

public class LogUtils {
	public static String describe(byte b[]) {
		if(b == null)
			return "null";
		StringBuffer sb = new StringBuffer("{ ");
		for(int i = 0; i < b.length; i ++)
			sb.append(String.valueOf(b[i])).append(" ");
		sb.append(" }");
		return sb.toString();
	}
	
	public static String describe(double b[]) {
		if(b == null)
			return "null";
		StringBuffer sb = new StringBuffer("{ ");
		for(int i = 0; i < b.length; i ++)
			sb.append(String.valueOf(b[i])).append(" ");
		sb.append(" }");
		return sb.toString();
	}
	
	public static String describe(CubeIndexEntry e) {
		return "IndexEntry{length = " + e.getLength() + ", data = " + describe(e.getData()) + "}";
	}
	
	public static String describeKey(byte key[]) {
		if(key.length == 0)
			return "{ }";
		StringBuilder sb = new StringBuilder("{");
		sb.append(Bytes.toLong(key, 0));
		for(int i = 1; i < key.length / 8; i ++)
			sb.append(" ").append(Bytes.toLong(key, i * 8));
		sb.append("}");
		return sb.toString();
	}
}
