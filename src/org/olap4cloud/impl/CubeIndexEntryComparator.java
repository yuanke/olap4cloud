package org.olap4cloud.impl;

import java.util.Comparator;

public class CubeIndexEntryComparator implements Comparator<CubeIndexEntry> {
	@Override
	public int compare(CubeIndexEntry o1, CubeIndexEntry o2) {
		return o1.compareTo(o2);
	}	
}
