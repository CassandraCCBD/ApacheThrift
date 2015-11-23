package org.apache.thrift;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Profiler {
	private static Logger logger = LoggerFactory.getLogger(Profiler.class);
	LinkedHashMap<ArrayList<Integer>,ArrayList<Integer>> mainList= new LinkedHashMap<ArrayList<Integer>,ArrayList<Integer>>();
	ArrayList<Float> avglist;
	ArrayList<Float> stddevlist;
        AtomicInteger hit= new AtomicInteger(0);
	AtomicInteger miss=new AtomicInteger(0);
	public void add_item(int currentRead,int currentWrite,int currentScan,int responseTime,int tag){
		   logger.debug("In add_item");	
                   ArrayList<Integer> keys=new ArrayList<Integer>();
		   keys.add(currentRead);
		   keys.add(currentScan);
		   keys.add(currentWrite);
		   if(check_hit(responseTime,keys,tag)==1)
			   hit.incrementAndGet();
		   else if(check_hit(responseTime,keys,tag)==0) 
			   miss.incrementAndGet();
		   
		   addToList(keys,(int)responseTime);
	  }
	public int check_hit(int responseTime,ArrayList<Integer> keys,int tag){
	           logger.debug("In check_hit");
		   float range_low,range_high;
		   if(tag==1){
		        try{ 
		   	ArrayList<Float> temp = Statistics.readPredictionList.get(keys);
		   	range_low = temp.get(0)-temp.get(1);
			range_high= temp.get(0)+temp.get(1);}
			catch(Exception e){
				return 2;
			}
		   }else{
		  	try 
			{
		   	ArrayList<Float> temp = Statistics.scanPredictionList.get(keys);
			range_low = temp.get(0)-temp.get(1);
			range_high= temp.get(0)+temp.get(1);
			}
			catch (Exception e)
			{
				return 2;
			}
		   }
		   
		   
		   if((float)responseTime< range_high && (float)responseTime > range_low){
		   	return 1;
		   }else
		   	return 0;
	}
	


	public void addToList(ArrayList<Integer> keys, Integer responseTime) {
	
		 ArrayList<Integer> temp = mainList.get(keys);

              if(temp == null) {
                       temp = new ArrayList<Integer>();
                       temp.add(responseTime);
                       mainList.put(keys, temp);
             } else {
                              if(!temp.contains(responseTime)) temp.add(responseTime);
             }
         }
         public float get_avg(ArrayList<Integer> list){
                 return get_sum(list)/list.size();
         }
         public int get_sum(ArrayList<Integer> list){
                 int total=0;
                 for(Integer i : list){
                         total+=i;
                 }
                 return total;
         }
         public float get_std_dev(ArrayList<Integer> list,  float mean){
                 float stddev=0;
                 for(Integer i : list){
                         stddev+=Math.pow((i-mean),2);
                 }
                 stddev/=list.size();
                 stddev=(float) Math.sqrt(stddev);
                 return stddev;
         }
         public void gen_result(){
                 avglist=new ArrayList<Float>();
                 stddevlist=new ArrayList<Float>();
                 int i=0;
                 for(ArrayList<Integer> key :mainList.keySet()){
                         avglist.add(get_avg(mainList.get(key)));
                         stddevlist.add(get_std_dev(mainList.get(key),avglist.get(i)));
                         
			 //logger.debug("Reads " + rws[0] +" Writes "+rws[1]+" Scans :"+rws[2]+" Avg "+avglist.get(i)+" Stddev "+stddevlist.get(i)+" Number of occurenc    es "+hash.get(key).size());
                 	i++;
		 }
         }

 }

	

