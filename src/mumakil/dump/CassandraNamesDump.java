import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.avro.Column;
import org.apache.cassandra.avro.ColumnOrSuperColumn;
import org.apache.cassandra.avro.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/*
  
  Dumps out select columns from every row in a column family as a tsv file to hdfs.
  
 */
public class CassandraNamesDump extends Configured implements Tool {
    public static class ColumnFamilyMapper extends Mapper<byte[], SortedMap<byte[], IColumn>, Text, Text> {
        public void map(byte[] key, SortedMap<byte[], IColumn> columns, Context context) throws IOException, InterruptedException {
            String names = "";
            for (IColumn column : columns.values()) {
                names += new String(column.name());
                names += "\t";
            }
            context.write(new Text(key), new Text(names));
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job                    = new Job(getConf());
        job.setJarByClass(CassandraNamesDump.class);
        job.setJobName("CassandraNamesDump");
        job.setNumReduceTasks(0);
        job.setMapperClass(ColumnFamilyMapper.class);        
        job.setInputFormatClass(ColumnFamilyInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Configuration conf = job.getConfiguration();
        ConfigHelper.setRangeBatchSize(conf, Integer.parseInt(conf.get("cassandra.batch_size")));
        ConfigHelper.setRpcPort(conf, conf.get("cassandra.thrift_port"));
        ConfigHelper.setInitialAddress(conf, conf.get("cassandra.initial_host"));
        ConfigHelper.setInputColumnFamily(conf, conf.get("cassandra.keyspace"), conf.get("cassandra.column_family"));

        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);
        ConfigHelper.setInputSlicePredicate(conf, predicate);

        // Handle output path
        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(0)));

        // Submit job to server and wait for completion
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CassandraNamesDump(), args);
        System.exit(0);
    }
}
