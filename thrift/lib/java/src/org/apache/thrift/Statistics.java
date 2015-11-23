package org.apache.thrift;


import java.util.*;
import java.util.concurrent.TimeUnit;
import java.io.*;

//import com.google.common.util.concurrent.Uninterruptibles;       
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Statistics 
{
	private static Logger logger = LoggerFactory.getLogger(PredictionClass.class);
	static LinkedHashMap<ArrayList<Integer>, ArrayList<Float>> readPredictionList;
	static LinkedHashMap<ArrayList<Integer>, ArrayList<Float>> writePredictionList; 
	static LinkedHashMap<ArrayList<Integer>, ArrayList<Float>> scanPredictionList ;

	static
	{
		readPredictionList = new LinkedHashMap<ArrayList<Integer>, ArrayList<Float>>();
		writePredictionList = new LinkedHashMap<ArrayList<Integer>, ArrayList<Float>>();
		scanPredictionList = new LinkedHashMap<ArrayList<Integer>, ArrayList<Float>>();
	}

}
