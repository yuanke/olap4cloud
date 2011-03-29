package org.olap4cloud.test;

import org.olap4cloud.client.CubeDescriptor;
import org.olap4cloud.client.CubeDimension;
import org.olap4cloud.client.CubeMeasure;
import org.olap4cloud.client.OLAPEngine;
import org.olap4cloud.impl.GenerateCubeIndexMR;
import org.olap4cloud.impl.GenerateCubeMR;

public class TestCubeUtils {

	public static void generateTestCube() throws Exception {
//		DataImportHFileMR.main(new String[]{});
		CubeDescriptor descr = TestCubeUtils.createTestCubeDescriptor();
		OLAPEngine engine = new OLAPEngine();
		engine.generateCube(descr);
	}
	
	public static CubeDescriptor createTestCubeDescriptor() throws Exception {
		CubeDescriptor descr = new CubeDescriptor();
		descr.loadFromClassPath("testcube.xml", TestCubeUtils.class.getClassLoader());
/*		descr.setSourceDataDir("/data");
		descr.setCubeName("testcube");
		descr.setAggregationsCount(1);
		descr.getDimensions().add(new CubeDimension("d1"));
		descr.getDimensions().add(new CubeDimension("d2"));
		descr.getDimensions().add(new CubeDimension("d3"));
		descr.getMeasures().add(new CubeMeasure("m1"));
		descr.getMeasures().add(new CubeMeasure("m2"));
		descr.getMeasures().add(new CubeMeasure("m3"));  */
		return descr;
	}

	public static void main(String args[]) throws Exception {
		generateTestCube();
	}
}
