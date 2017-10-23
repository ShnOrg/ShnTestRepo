package mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

public class MariatalStatusMapper extends TableMapper<ImmutableBytesWritable,IntWritable> {

  private ImmutableBytesWritable key = new ImmutableBytesWritable();
  private IntWritable ONE = new IntWritable(1);

  public void map(ImmutableBytesWritable row,Result value,Context context)
      throws IOException, InterruptedException {
      key.set(value.getValue("personal".getBytes(),"mariatal_status".getBytes()));
      context.write(key,ONE);
  }

}
