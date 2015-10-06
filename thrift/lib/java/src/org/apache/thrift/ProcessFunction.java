/**
 * 
 */
package org.apache.thrift;

import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.Profiling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProcessFunction<I, T extends TBase> {
  private final String methodName;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFunction.class.getName());

  public ProcessFunction(String methodName) {
    this.methodName = methodName;
  }

  public final void process(int seqid, TProtocol iprot, TProtocol oprot, I iface) throws TException {
    T args = getEmptyArgsInstance();
    long startTime=0, endTime=0 ;
    int currentops=0, tag=0, endops=0 ;// stores the current number of reads/writes
    try {
      args.read(iprot);
      LOGGER.debug("Cassandra Team: Args {}", args);
      // initialize start time here 
      startTime =TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
      // incrementAndGetRead or Write or Scan
      if (args.getQosReq()==10)
      {
      	tag=1;
	while (Profiling.numRead.get()>5)
	{}
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
    Profiling.writeToFile(tag,currentops,responseTime, endops);
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
