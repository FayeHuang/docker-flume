package org.apache.flume.sink.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.apache.flume.conf.ComponentConfiguration;

public class SplittingSerializer implements AsyncHbaseEventSerializer {
  private byte[] table;
  private byte[] cf;
  private byte[] payload;
  private String[] payloadColumns;
  
  @Override
  public void initialize(byte[] table, byte[] cf) {
    this.table = table;
    this.cf = cf;
  }

  @Override
  public List<PutRequest> getActions() {
    List<PutRequest> actions = new ArrayList<PutRequest>();
    if(payloadColumns.length > 0){
      try {
        String colValueStr = new String(payload);
        String[] colValues = colValueStr.split(",");
        String rowKey = colValues[0]+colValues[1]+colValues[9];
        for(int i = 0; i < colValues.length; i++)
        {
          PutRequest putRequest =  new PutRequest(table, rowKey.getBytes(), cf,
              payloadColumns[i].getBytes(), colValues[i].getBytes());
          actions.add(putRequest);
        }
      } catch (Exception e){
        throw new FlumeException("Could not add put request", e);
      }
    }
    return actions;
  }

  @Override
  public List<AtomicIncrementRequest> getIncrements(){
    List<AtomicIncrementRequest> actions = new
        ArrayList<AtomicIncrementRequest>();
    actions.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), cf, "eventCount".getBytes()));
    return actions;
  }

  @Override
  public void cleanUp() {
    table = null;
    cf = null;
    payload = null;
    payloadColumns = null;
  }

  @Override
  public void configure(Context context) {
    String pCol = context.getString("columns", "pCol");
    payloadColumns = pCol.split(",");
  }

  @Override
  public void setEvent(Event event) {
    this.payload = event.getBody();
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // TODO Auto-generated method stub
  }
}
  
