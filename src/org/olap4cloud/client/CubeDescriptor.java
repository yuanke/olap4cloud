package org.olap4cloud.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.olap4cloud.impl.AggregationCubeDescriptor;
import org.olap4cloud.impl.CubeScanAggregate;
import org.olap4cloud.impl.OLAPEngineConstants;
import org.olap4cloud.impl.aggr.CountCubeScanAggregate;
import org.olap4cloud.impl.aggr.MaxCubeScanAggregate;
import org.olap4cloud.impl.aggr.MinCubeScanAggregate;
import org.olap4cloud.impl.aggr.SumCubeScanAggregate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class CubeDescriptor implements Serializable {
	
	static Logger logger = Logger.getLogger(CubeDescriptor.class);
	
	String cubeName;
	
	String cubeDataTable;
	
	String cubeIndexTable;
	
	String sourceDataDir;
	
	List<CubeMeasure> measures = new ArrayList<CubeMeasure>();
	
	List<CubeDimension> dimensions = new ArrayList<CubeDimension>();
	
	List<AggregationCubeDescriptor> aggregationCubes = null;
	
	int aggregationsCount = -1;

	public List<CubeMeasure> getMeasures() {
		return measures;
	}

	public void setMeasures(List<CubeMeasure> measures) {
		this.measures = measures;
	}

	public List<CubeDimension> getDimensions() {
		return dimensions;
	}

	public void setDimensions(List<CubeDimension> dimensions) {
		this.dimensions = dimensions;
	}
	
	public void setCubeName(String cubeName) {
		this.cubeName = cubeName;
		this.cubeDataTable = cubeName + OLAPEngineConstants.DATA_CUBE_NAME_SUFFIX;
		this.cubeIndexTable = cubeDataTable + OLAPEngineConstants.CUBE_INDEX_SUFFIX;
	}

	public String getCubeDataTable() {
		return cubeDataTable;
	}

	public String getCubeIndexTable() {
		return cubeIndexTable;
	}
	
	public String getSourceDataDir() {
		return sourceDataDir;
	}

	public void setSourceDataDir(String sourceDataDir) {
		this.sourceDataDir = sourceDataDir;
	}
	
	public int getAggregationsCount() {
		return aggregationsCount;
	}

	public void setAggregationsCount(int aggregationsCount) {
		this.aggregationsCount = aggregationsCount;
	}
	
	public String getCubeName() {
		return cubeName;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Cube[name = ")
			.append(cubeName)
			.append(" measures = {");
		for(CubeMeasure measure: measures)
			sb.append("(name = ").append(measure.getName()).append(") ");
		return sb.toString();
	}
	
	public void loadFromClassPath(String resourceName, ClassLoader classLoader) throws OLAPEngineException {
		InputStream in = null;
		try {
			in = classLoader.getResourceAsStream(resourceName);
			if(in == null)
				throw new OLAPEngineException("Can't find " + resourceName + " in classpath.");
			load(in);
		} finally {
			try {
				if(in != null)
					in.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
				throw new OLAPEngineException(e);
			}
		}
	}
	
	public void loadFromLocalFS(String filePath) throws OLAPEngineException {
		FileInputStream in = null;
		try {
			in = new FileInputStream(filePath);
			load(in);
		} catch(Exception e) {
			logger.debug(e.getMessage(), e);
			throw new OLAPEngineException(e);
		} finally {
			if(in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.debug(e.getMessage(), e);
					throw new OLAPEngineException(e);
				}
		}
	}
	
	public void load(InputStream in) throws OLAPEngineException {
		String methodName = "load() ";
		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory
					.newInstance();
			documentBuilderFactory.setIgnoringComments(true);
			documentBuilderFactory.setNamespaceAware(true);
			try {
				documentBuilderFactory.setXIncludeAware(true);
			} catch (UnsupportedOperationException e) {
				logger.error("Failed to set setXIncludeAware(true) for parser "
						+ documentBuilderFactory + ":" + e, e);
			}
			DocumentBuilder documentBuilder = documentBuilderFactory
					.newDocumentBuilder();
			Document document = documentBuilder.parse(in);
			Element root = document.getDocumentElement();
			if(!"cube".equalsIgnoreCase(root.getTagName())) 
				throw new OLAPEngineException("Bad configuration file: top-level element is not <cube>");
			String cubeName = root.getAttribute("name");
			if(cubeName == null)
				throw new OLAPEngineException("Bad configuration file: <cube> element does not have 'name' attribute");
			setCubeName(cubeName);
			String sourcePath = root.getAttribute("sourcePath");
			if(sourcePath == null)
				throw new OLAPEngineException("Bad configuration file: <cube> element does not have 'sourcePath' " +
						"attribute");
			setSourceDataDir(sourcePath);
			String sAggregationsCount = root.getAttribute("aggregationsCount");
			if(sAggregationsCount == null)
				throw new OLAPEngineException("Bad configuration file: <cube> element does not have 'aggregationsCount' " +
						"attribute");
			setAggregationsCount(Integer.parseInt(sAggregationsCount));
			NodeList dimensions = root.getElementsByTagName("dimension");
			for(int i = 0; i < dimensions.getLength(); i ++) {
				Element dimension = (Element)dimensions.item(i);
				String name = dimension.getAttribute("name");
				if(name == null)
					throw new OLAPEngineException("Bad configuration file: <dimension> element does not have " +
							"'name' attribute.");
				getDimensions().add(new CubeDimension(name));
			}
			NodeList measures = root.getElementsByTagName("measure");
			for(int i = 0; i < measures.getLength(); i ++) {
				Element measure = (Element)measures.item(i);
				String name = measure.getAttribute("name");
				if(name == null)
					throw new OLAPEngineException("Bad configuration file: <measure> element does not have " +
							"'name' attribute.");
				getMeasures().add(new CubeMeasure(name));
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new OLAPEngineException(e);
		}
	}
	
	public List<AggregationCubeDescriptor> getAggregationCubes() throws OLAPEngineException {
		if(aggregationCubes == null)
			initAggregationCubes();
		return aggregationCubes;
	}

	private synchronized void initAggregationCubes() throws OLAPEngineException {
		String methodName = "initAggregationCubes() ";
		if(aggregationCubes != null)
			return;
		aggregationCubes = new ArrayList<AggregationCubeDescriptor>();
		for(int i = getDimensions().size(); i > 0; i --) {
			List<Integer> dimensionIndexes = new ArrayList<Integer>();
			for(int curDimension = 0; curDimension <= getDimensions().size(); curDimension ++)
				generateAggregationCubesWithNumberOfDimensions(i, curDimension, dimensionIndexes);
		}
		if(logger.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			for(CubeDescriptor descr: getAggregationCubes())
				sb.append(" ").append(descr.getCubeName());
			logger.debug(methodName + "generated aggregation cubes: " + sb.toString());
		}
	}

	private void generateAggregationCubesWithNumberOfDimensions(int numberOfDimensions,
			int curDimension, List<Integer> dimensionIndexes) throws OLAPEngineException {
		if(getAggregationsCount() != -1 && getAggregationCubes().size() == getAggregationsCount())
			return;
		if(curDimension >= getDimensions().size())
			return;
		dimensionIndexes.add(curDimension);
		if(dimensionIndexes.size() == numberOfDimensions) {
			AggregationCubeDescriptor aggCubeDescriptor = new AggregationCubeDescriptor();
			StringBuilder aggCubeName = new StringBuilder(getCubeName());
			aggCubeName.append("_aggregate");
			for(CubeMeasure measure: getMeasures()) {
				aggCubeDescriptor.getAggregates().add(new SumCubeScanAggregate("sum(" + measure.getName() + 
						")", this));
				aggCubeDescriptor.getAggregates().add(new MinCubeScanAggregate("min(" + measure.getName() + 
						")", this));
				aggCubeDescriptor.getAggregates().add(new MaxCubeScanAggregate("max(" + measure.getName() + 
						")", this));
				aggCubeDescriptor.getAggregates().add(new CountCubeScanAggregate("count(" + measure.getName() + 
						")", this));
			}
			for(CubeScanAggregate aggregate: aggCubeDescriptor.getAggregates()) 
				aggCubeDescriptor.getMeasures().add(new CubeMeasure(aggregate.getColumnName() + 
						"_" + aggregate.getAggregateName()));
			for(int i: dimensionIndexes)
				aggCubeDescriptor.getDimensions().add(new CubeDimension(getDimensions().get(i).getName()));
			for(CubeDimension dimension: aggCubeDescriptor.getDimensions())
				aggCubeName.append("_" + dimension.getName());
			aggCubeDescriptor.setCubeName(aggCubeName.toString());
			getAggregationCubes().add(aggCubeDescriptor);
			dimensionIndexes.remove(dimensionIndexes.size() - 1);
			return;
		}
		for(int nextDimension = curDimension + 1; nextDimension < getDimensions().size(); nextDimension ++)
			generateAggregationCubesWithNumberOfDimensions(numberOfDimensions, nextDimension, dimensionIndexes);
		dimensionIndexes.remove(dimensionIndexes.size() - 1);
	}
	
	public boolean containsDimensions(String dimName) {
		for(CubeDimension dim: getDimensions())
			if(dimName.equals(dim.getName()))
				return true;
		return false;
	}
}
