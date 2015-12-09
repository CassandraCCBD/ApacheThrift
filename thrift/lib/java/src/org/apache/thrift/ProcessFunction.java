/**
 * 
 */
package org.apache.thrift;

import java.util.concurrent.TimeUnit;
import java.util.Vector;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.Profiling;
import org.apache.thrift.Profiler;
import org.apache.thrift.GlobalProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/* adding some things so that we add things to ScanStage */
/* this didn't work cuz Cassandra isn't in this thing's classpath
 * We're going to try this from Cassandra's end and try and make an object from there */
//import org.apache.cassandra.concurrent.*;

class GetLibraries {
	private static final Logger logger = LoggerFactory.getLogger(GetLibraries.class);
	private static java.lang.reflect.Field LIBRARIES;	
	public static String[] getLoadedLibraries(final ClassLoader loader) 
	{
		try 
		{
			LIBRARIES = ClassLoader.class.getDeclaredField("loadedLibraryNames");
			LIBRARIES.setAccessible(true);
			 final Vector<String> libraries = (Vector<String>) LIBRARIES.get(loader);
		         return libraries.toArray(new String[] {});
		}
		catch (Exception e)
		{
			logger.debug("Exception: ", e);
			String[] array = new String[1];
			array[0] = "Error";
			return array;
		}
        }
}
public abstract class ProcessFunction<I, T extends TBase> {
  private final String methodName;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFunction.class.getName());

  public ProcessFunction(String methodName) {
    this.methodName = methodName;
  }
  public native int HelloWorld();
  public static Profiler Read;
  public static Profiler Scan;
  public static GlobalProfiler GScan;
  public static GlobalProfiler GRead;
  public static GlobalProfiler GWrite;
  public static Profiler Write;
  public static PredictionClass threadObject;
  static 
  {
  	LOGGER.debug("Going to load the native library");
	try 
	{
		System.loadLibrary("hello");
	}
	catch (Exception e)
	{
		LOGGER.debug("couldn't load the library");
	}
	Read = new Profiler();
	Scan= new Profiler();
	Write= new Profiler();
	GRead = new GlobalProfiler();
	GScan= new GlobalProfiler();
	GWrite= new GlobalProfiler();
	LOGGER.debug("Going to submit the thread to ScanStage");
	try 
	{
		throw new RuntimeException("Trying to see who calls ProcessFunction's Static");
	}
	catch(Exception e)
	{
		LOGGER.debug("StackTrace ", e);
	}
	/*threadObject = new PredictionClass();
	StageManager.getStage(Stage.SCAN).execute(threadObject);*/
  }
  public int returnThread()
  {	
  	final String[] libraries = GetLibraries.getLoadedLibraries(ClassLoader.getSystemClassLoader());
	LOGGER.debug("CASSANDRA TEAM:List of libraries");
	for (int i=0;i<libraries.length;i++)
				LOGGER.debug(libraries[i]);
  	int id = HelloWorld();
	LOGGER.debug("Got the id" + id);
	return id;
  }

  public final void process(int seqid, TProtocol iprot, TProtocol oprot, I iface) throws TException {
    T args = getEmptyArgsInstance();
    long startTime=0, endTime=0 ;
    int numtot=0;
    int currentRead=0, tag=0, endops=0 , currentWrite=0, currentScan=0;// stores the current number of reads/writes
    int currentLocal=0, currentScanLocal=0, currentNonLocalRead=0, currentReadStage=0;
    try {
      args.read(iprot);
      LOGGER.debug("Cassandra Team: Args {}", args);
      int id = returnThread();
      LOGGER.debug("Thread ID in Thrift - " + id);
      // initialize start time here 
      startTime =TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
      numtot=Profiling.numTot.get();
      // incrementAndGetRead or Write or Scan
      LOGGER.debug("args.getQosReq {} ",args.getQosReq());
      if (args.getQosReq()==10)
      {
      	tag=1;
	//Adding to Cgroups
	/*try{
		LOGGER.debug("Sending to group1 ");
		Runtime.getRuntime().exec("cgclassify -g cpu:/group1 "+id);
	}catch(Exception e){
		LOGGER.debug("not sent to cgroup "+e);
	}*/
	/* initializing all the Profiling stats */
      	currentRead=Profiling.incrementAndGetRead();
	currentWrite=Profiling.numWrite.get();
	currentScan=Profiling.numScan.get();
	/*currentLocal=Profiling.localRead.get();
	currentScanLocal= Profiling.localScan.get();
	currentNonLocalRead = Profiling.totalRead.get() - Profiling.localRead.get();
	currentReadStage = Profiling.numRead.get();*/
      }
      else if (args.getQosReq()==15)
      {
      	tag=3;
	//Adding to Cgroups
	/* we make all scans sleep to see if it makes a difference for Reads */
	try 
	{
		double ratio =0;	
		if (Profiling.numScan.get()!=0)
			ratio = Profiling.numRead.get()/Profiling.numScan.get();
	/*	if(ratio >= 1){
			LOGGER.debug("Scan going to sleep");
			Thread.currentThread().sleep(5000);
		} 
		else 
		{
			LOGGER.debug("Scan going through, ratio is " + ratio);
			LOGGER.debug("CurrentRead - " + Profiling.numRead.get() + " currentScan " + Profiling.numScan.get());
		} */
		//Thread.currentThread().sleep(10000);
	}
	catch (Exception e)
	{
	
	}
/*	try{
		LOGGER.debug("Sending to group3 ");
		Runtime.getRuntime().exec("cgclassify -g cpu:/group3 "+id);
	}catch(Exception e){
		LOGGER.debug("not sent to cgroup "+e);
	}*/
      	currentScan=Profiling.incrementAndGetScan();
	currentWrite=Profiling.numWrite.get();
	currentRead=Profiling.numRead.get();
	/*currentLocal=Profiling.localRead.get();
	currentScanLocal= Profiling.localScan.get();
	currentNonLocalRead = Profiling.totalRead.get() - Profiling.localRead.get();*/
      }
      else if (args.getQosReq()==5)
      {
      	tag=2;
	//Adding to Cgroups
/*	try{
		LOGGER.debug("Sending to group2 ");
		Runtime.getRuntime().exec("cgclassify -g cpu:/group2 "+id);
	}catch(Exception e){
		LOGGER.debug("not sent to cgroup "+e);
	}*/
      	currentWrite=Profiling.incrementAndGetWrite();
	currentRead=Profiling.numRead.get();
	currentScan=Profiling.numScan.get();
	/*currentLocal=Profiling.localRead.get();
	currentScanLocal= Profiling.localScan.get();
	currentNonLocalRead = Profiling.totalRead.get() - Profiling.localRead.get();*/
      }
      	
    } catch (TProtocolException e) {
      iprot.readMessageEnd();
      TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
      oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
      x.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
      return;
    }
    iprot.readMessageEnd();
    TBase result = null;
    
    try {
      result = getResult(iface, args);
    } catch(TException tex) {
      LOGGER.error("Internal error processing " + getMethodName(), tex);
      TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR, 
        "Internal error processing " + getMethodName());
      oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
      x.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
      return;
    }
    // initialize endTime
    endTime =TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    int responseTime = (int) (endTime -startTime);
    // call the writeToFile method
    LOGGER.debug("going to call the write function");
    if (tag==1)
    {
    	    GRead.addToList((long)responseTime);
    	    Read.add_item(currentRead, currentWrite, currentScan, responseTime,tag);
	    endops=Profiling.decrementRead();
    }
    else if (tag==2)
    {
    	    GWrite.addToList((long)responseTime);
    	    Write.add_item(currentRead, currentWrite, currentScan, responseTime, tag);
	    endops=Profiling.decrementWrite();
    }
    else if (tag==3)
    {
    	    GScan.addToList((long)responseTime);
    	    Scan.add_item(currentRead, currentWrite, currentScan, responseTime,tag);
	    endops=Profiling.decrementScan();
    }
    /*Profiling.writeToFile(tag,currentRead, currentWrite, currentScan, responseTime, currentLocal, currentScanLocal, currentReadStage);*/

    if(!isOneway()) {
      oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.REPLY, seqid));
      result.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
    }
  }

  protected abstract boolean isOneway();

  public abstract TBase getResult(I iface, T args) throws TException;

  public abstract T getEmptyArgsInstance();

  public String getMethodName() {
    return methodName;
  }
}
