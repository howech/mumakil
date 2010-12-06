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
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;

/*
  
  Dumps out select columns from every row in a column family as a tsv file to hdfs.
  
 */
public class LoadTable extends Configured implements Tool {
    
    public static class ColumnFamilyMapper extends Mapper<LongWritable, Text, ByteBuffer, List<Mutation>> {
        
        private List<Mutation> rowMutationList = new ArrayList<Mutation>();
        private Integer keyField;
        private Integer tsField;
        private String[] fieldNames;
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            Long timeStamp;
            if (tsField == -1) {
                timeStamp = System.currentTimeMillis() * 1000;
            } else {
                try {
                    timeStamp = Long.parseLong(fields[tsField]);
                } catch (NumberFormatException e) {
                    System.out.println("Bad timestamp: ["+fields[tsField]+"], using current time");
                    timeStamp = System.currentTimeMillis() * 1000;
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("Timestamp field: ["+tsField+"] doesn't exist, using current time");
                    timeStamp = System.currentTimeMillis() * 1000;
                }
            }
            
            for(int i = 0; i < fields.length; i++) {
                if (i < fieldNames.length && i != keyField) {
                    rowMutationList.add(CassandraUtils.getMutation(fieldNames[i], fields[i], timeStamp));
                    context.write(ByteBuffer.wrap(fields[keyField].getBytes()), rowMutationList);
                    rowMutationList.clear();
                }
            }
        }

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            
            this.fieldNames   = conf.get("cassandra.column_names").split(",");
            this.keyField     = Integer.parseInt(conf.get("cassandra.row_key_field"));

            /* Deal with custom timestamp field */
            try {
                this.tsField = Integer.parseInt(conf.get("cassandra.timestamp_field"));
            } catch (NumberFormatException e) {
                this.tsField = -1;
            }
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job                    = new Job(getConf()); 
        job.setJarByClass(LoadTable.class);
        job.setJobName("LoadTable");
        job.setMapperClass(ColumnFamilyMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(ByteBuffer.class);
        job.setOutputValueClass(List.class);
        job.setOutputFormatClass(ColumnFamilyOutputFormat.class);

        Configuration conf = job.getConfiguration();
        ConfigHelper.setRpcPort(conf, conf.get("cassandra.thrift_port"));
        ConfigHelper.setInitialAddress(conf, conf.get("cassandra.initial_host"));
        ConfigHelper.setOutputColumnFamily(conf, conf.get("cassandra.keyspace"), conf.get("cassandra.column_family"));
        ConfigHelper.setPartitioner(conf, "org.apache.cassandra.dht.RandomPartitioner");
        // Handle input path
        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));

        // Submit job to server and wait for completion
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new LoadTable(), args);
        System.exit(0);
    }
}
