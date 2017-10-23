package mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class Driver {

  public static void main(String x[])
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = HBaseConfiguration.create();
  //  configuration.set("fs.defaultFS", "hdfs://localhost:8020");
    Job job = Job.getInstance(configuration);
    job.setJarByClass(Driver.class);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.initTableMapperJob("census",scan,MariatalStatusMapper.class,
        ImmutableBytesWritable.class, IntWritable.class,job);
    TableMapReduceUtil.initTableReducerJob("summary",MariatalStatusReducer.class,job);
    job.setNumReduceTasks(10);
    job.waitForCompletion(true);

  }
}
