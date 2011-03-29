package org.olap4cloud.client;

public class OLAPEngineException extends Exception {
	public OLAPEngineException(String string) {
		super(string);
	}
	
	public OLAPEngineException(Exception e) {
		super(e);
	}
}
