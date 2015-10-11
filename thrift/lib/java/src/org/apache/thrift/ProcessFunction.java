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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    int currentops=0, tag=0, endops=0 ;// stores the current number of reads/writes
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
/*	while (Profiling.numRead.get()>5)
	{} */
	//Adding to Cgroups
	try{
		LOGGER.debug("Sending to group1 ");
		Runtime.getRuntime().exec("cgclassify -g cpu:/group1 "+id);
	}catch(Exception e){
		LOGGER.debug("not sent to cgroup "+e);
	}


      	currentops=Profiling.incrementAndGetRead();
      }
      else if (args.getQosReq()==15)
      {
      	tag=3;
      	currentops=Profiling.incrementAndGetScan();
      }
      else if (args.getQosReq()==5)
      {
      	tag=2;
      	currentops=Profiling.incrementAndGetWrite();
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
    long responseTime = endTime -startTime;
    // call the writeToFile method
    LOGGER.debug("going to call the write function");
    if (tag==1)
	    endops=Profiling.decrementRead();
    else if (tag==2)
	    endops=Profiling.decrementWrite();
    else if (tag==3)
	    endops=Profiling.decrementScan();
    Profiling.writeToFile(tag,currentops,responseTime, endops,numtot);
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
