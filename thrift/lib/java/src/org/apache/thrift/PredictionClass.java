package org.apache.thrift;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.io.*;

//import com.google.common.util.concurrent.Uninterruptibles;       
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PredictionClass implements Runnable
{
	private static Logger logger = LoggerFactory.getLogger(PredictionClass.class);
	/** running a single thread does not work as the thread continously dies
	 * So we run multiple threads and synchronize them using semaphores
	 */
	private static Semaphore sem = new Semaphore(1);
	/**
	 * We used to have all the data corresponding to the Profiling here but since multiple classes
	 * may need to use it, we've moved it to another class completely
	 */
	
/*	LinkedHashMap<ArrayList<Integer>, ArrayList<Float>> Statistics.readPredictionList;
	LinkedHashMap<ArrayList<Integer>, ArrayList<Float>> Statistics.writePredictionList; 
	LinkedHashMap<ArrayList<Integer>, ArrayList<Float>> Statistics.scanPredictionList ; */

	public 	PredictionClass()
	{
	/*	Statistics.readPredictionList = new LinkedHashMap<ArrayList<Integer>, ArrayList<Float>>();
		Statistics.writePredictionList = new LinkedHashMap<ArrayList<Integer>, ArrayList<Float>>();
		Statistics.scanPredictionList = new LinkedHashMap<ArrayList<Integer>, ArrayList<Float>>(); */
	}

	public void run()
	{
		/* this fellow sits and calculates all average values and stddev stuff, put them in a Hashmap */
		/*Calculating for Read */
		float average, stddev;
		int argument=0;
		while (true)
		{
			try
			{
				Thread.sleep(100);
			}
			catch(Exception e)
			{
				logger.debug("ERROR in sleeping", e);
			}
			try
			{
				/** we acquire the semaphore to do the calculation 
				 */
				sem.acquire();
			}
			catch(Exception e)
			{
				logger.debug("Error in acquiring semaphore", e);
			}
			logger.debug(" In run of Predict ");
			//Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);    
			try 
			{
				argument = argument%5;
				for (ArrayList<Integer> key: ProcessFunction.Read.mainList.keySet())
				{
					ArrayList<Float> valueList = new ArrayList<Float>();
					average = ProcessFunction.Read.get_avg(ProcessFunction.Read.mainList.get(key));
					stddev = ProcessFunction.Read.get_std_dev(ProcessFunction.Read.mainList.get(key), average);
					valueList.add(average);
					valueList.add(stddev);
					valueList.add((float)(ProcessFunction.Read.mainList.get(key).size()));
					Statistics.readPredictionList.put(key, valueList);
				}

				for (ArrayList<Integer> key: ProcessFunction.Write.mainList.keySet())
				{
					ArrayList<Float> valueList = new ArrayList<Float>();
					average = ProcessFunction.Write.get_avg(ProcessFunction.Write.mainList.get(key));
					stddev = ProcessFunction.Write.get_std_dev(ProcessFunction.Write.mainList.get(key), average);
					valueList.add(average);
					valueList.add(stddev);
					valueList.add((float)(ProcessFunction.Write.mainList.get(key).size()));
					Statistics.writePredictionList.put(key, valueList);
				}

				for (ArrayList<Integer> key: ProcessFunction.Scan.mainList.keySet())
				{
					ArrayList<Float> valueList = new ArrayList<Float>();
					average = ProcessFunction.Scan.get_avg(ProcessFunction.Scan.mainList.get(key));
					stddev = ProcessFunction.Scan.get_std_dev(ProcessFunction.Scan.mainList.get(key), average);
					valueList.add(average);
					valueList.add(stddev);
					valueList.add((float)(ProcessFunction.Scan.mainList.get(key).size()));
					Statistics.scanPredictionList.put(key, valueList);
				}
				printToFile(argument++);	
				/** we also call a function to calculate the mean of the overall reads writes 
				 *  and scans 
				 *  This is just to verify if each procedure is working the way it should */
			/*	long sum = 0;
				long readAvg, scanAvg, writeAvg;
				try 
				{
						
				}
				catch (Exception e)
				{
					logger.debug("Exception in calculation ", e);
				} */
				overallPrint();

			}
			catch(Exception e)
			{
				logger.debug("Error in calculation ",e);
			}
			try
			{
			/** We release the semaphore to ensure that other guys in the threadpool can use this
			 */
			sem.release();
			}
			catch(Exception e)
			{
				logger.debug("Exception in releasing semaphore");
			}
		}

	}


	public void overallPrint()
	{
		try
		{
			PrintWriter write = new PrintWriter ("/root/overallStuff");
			write.println("Overall Read: " + ProcessFunction.GRead.average + " Number of reads " + ProcessFunction.GRead.responseTimeList.size()) ;
			write.println("Overall Scan: " + ProcessFunction.GScan.average + " Number of scans" + ProcessFunction.GScan.responseTimeList.size()) ;
			//write.println("Overall Write: " + writeAvg);
			write.close();
		}
		catch(Exception e)
		{}
	}

	public void printToFile(int argument)
	{
		try
		{
			PrintWriter write = new PrintWriter ("/root/newStuff"+argument);
			write.println("For Read");
			write.println("Number of hits " + ProcessFunction.Read.hit.get());
			write.println("Number of miss " + ProcessFunction.Read.miss.get());
			for (ArrayList<Integer> key: Statistics.readPredictionList.keySet())
			{
				write.println("Key : "+ key +" values :"+ Statistics.readPredictionList.get(key));
			}

			write.println("For Scan");
			write.println("Number of hits " + ProcessFunction.Scan.hit.get());
			write.println("Number of miss " + ProcessFunction.Scan.miss.get());
			for (ArrayList<Integer> key: Statistics.scanPredictionList.keySet())
			{
				write.println("Key : "+ key+ " values :"+ Statistics.scanPredictionList.get(key));
			}
			
			write.println("For Write");
			write.println("Number of hits " + ProcessFunction.Write.hit.get());
			write.println("Number of miss " + ProcessFunction.Write.miss.get());
			for (ArrayList<Integer> key: Statistics.writePredictionList.keySet())
			{
				write.println("Key : "+ key+ " values :"+ Statistics.writePredictionList.get(key));
			}
			write.close();
		}
		catch(Exception e)
		{ 
			logger.debug("PROFILING FILE EXCEPTION");	
		}
			logger.debug("For Read");
			logger.debug("Number of hits " + ProcessFunction.Read.hit.get());
			logger.debug("Number of miss " + ProcessFunction.Read.miss.get());
			for (ArrayList<Integer> key: Statistics.readPredictionList.keySet())
			{
				logger.debug("Key : "+ key +" values :"+ Statistics.readPredictionList.get(key));
			}

			logger.debug("For Scan");
			logger.debug("Number of hits " + ProcessFunction.Scan.hit.get());
			logger.debug("Number of miss " + ProcessFunction.Scan.miss.get());
			for (ArrayList<Integer> key: Statistics.scanPredictionList.keySet())
			{
				logger.debug("Key : "+ key+ " values :"+ Statistics.scanPredictionList.get(key));
			}
			
			logger.debug("For Write");
			logger.debug("Number of hits " + ProcessFunction.Write.hit.get());
			logger.debug("Number of miss " + ProcessFunction.Write.miss.get());
	}
}



