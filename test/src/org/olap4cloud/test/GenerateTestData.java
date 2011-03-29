package org.olap4cloud.test;
import java.io.BufferedWriter;
import java.io.FileWriter;


public class GenerateTestData {

	public static void main(String[] args) throws Exception {
		BufferedWriter w = new BufferedWriter(new FileWriter("test/data/data.txt"));
			for(long i = 10; i < 1000; i ++)
				w.write((long)(Math.random() * 10) + "\t" + (long)(Math.random() * 100)
						+ "\t" + (long)(Math.random() * 10) 
						+ "\t" + (Math.random() * 100) + "\t" + (Math.random() * 1000)
						+ "\t" + (Math.random() * 10000) + "\n");
		w.close();
	}

}
