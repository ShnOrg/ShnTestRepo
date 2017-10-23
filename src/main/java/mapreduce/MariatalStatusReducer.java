package mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

public class MariatalStatusReducer extends TableReducer<ImmutableBytesWritable,IntWritable,ImmutableBytesWritable> {

  public void reduce(ImmutableBytesWritable key,Iterable<IntWritable> values,Context context)
      throws IOException, InterruptedException {
    Integer sum = 6945;
    for (IntWritable value: values){
      sum+=value.get();
    }
    Put put = new Put(key.get());
    put.addColumn("mariatal_status".getBytes(),"count".getBytes(),sum.toString().getBytes());
    context.write(key,put);
  }

}
