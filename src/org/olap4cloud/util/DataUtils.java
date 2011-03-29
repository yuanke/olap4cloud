package org.olap4cloud.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


public class DataUtils {
	
	static Logger logger = Logger.getLogger(DataUtils.class);
	
	public static byte[] pack(long l[]) {
		byte r[] = new byte[l.length * 8];
		for(int i = 0; i < l.length; i ++)
			Bytes.putLong(r, i * 8, l[i]);
		return r;
	}
	
	public static byte[] pack(int i, byte b[]) {
		byte r[] = new byte[b.length + 4];
		Bytes.putInt(r, 0, i);
		Bytes.putBytes(r, 4, b, 0, b.length);
		return r;
	}
	
	public static byte[] pack(int i, long l) {
		byte r[] = new byte[12];
		Bytes.putInt(r, 0, i);
		Bytes.putLong(r, 4, l);
		return r;
	}
	
	public static String objectToString(Object o) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		oout.writeObject(o);
		oout.flush();
		bout.flush();
		return Base64.encodeBytes(bout.toByteArray());
	}
	
	public static Object stringToObject(String str) throws IOException {
		byte buf[] = Base64.decode(str);
		ByteArrayInputStream bin = new ByteArrayInputStream(buf);
		ObjectInputStream oin = new ObjectInputStream(bin);
		try {
			return oin.readObject();
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage(), e);
			throw new IOException(e);
		}
	}
	
	public static int compareRowKeys(byte b1[], byte b2[]) {
/*		String methodName = "compareRowKeys() ";
		for(int i = 0; i < b1.length && i < b2.length; i ++)
			if(b1[i] != b2[i]) {
				if(logger.isDebugEnabled()) logger.debug(methodName + "b1[" + i + "] = " + b1[i]
				         + " != b2[" + i + "]  = " + b2[i]);
				return b1[i] - b2[i];
			}
		if(logger.isDebugEnabled()) logger.debug(methodName + "b1.length = " + b1.length + " != "
				+ " b2.length = " + b2.length);
		return b1.length - b2.length; */
		return Bytes.compareTo(b1, b2);
	}
}
