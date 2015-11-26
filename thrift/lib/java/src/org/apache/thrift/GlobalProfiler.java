package org.apache.thrift;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GlobalProfiler 
{
	private static Logger logger = LoggerFactory.getLogger(Profiler.class);
	ArrayList <Long> responseTimeList;
	long sum;
	long average;	
	GlobalProfiler()
	{
		responseTimeList = new ArrayList<Long>();
		sum=0;
		average=0;
	}
	
	void addToList(long value)
	{
		responseTimeList.add(value);
		sum+=value;
		average = sum/responseTimeList.size();
	}
}
