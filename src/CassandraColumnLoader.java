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

public class CassandraColumnLoader extends Configured implements Tool {
    
    public static class ColumnFamilyMapper extends Mapper<LongWritable, Text, ByteBuffer, List<Mutation>> {
        
        private List<Mutation> wholeRow = new ArrayList<Mutation>();
        private Integer keyField;
        private String fillValue;
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            for(int i = 0; i < fields.length; i++) {
                if (i != keyField) {
                    wholeRow.add(getMutation(fields[i], fillValue, System.currentTimeMillis() * 1000));
                }
            }
            context.write(ByteBuffer.wrap(fields[keyField].getBytes()), wholeRow);
            wholeRow.clear();
        }

        private static Mutation getMutation(String name, String value, Long timeStamp) {
            Mutation m = new Mutation();
            m.column_or_supercolumn = getCoSC(name, value, timeStamp);
            return m;
        }

        private static ColumnOrSuperColumn getCoSC(String name, String value, Long timeStamp) {
            ByteBuffer columnName  = ByteBuffer.wrap(name.getBytes());
            ByteBuffer columnValue = ByteBuffer.wrap(value.getBytes());

            Column c    = new Column();
            c.name      = columnName;
            c.value     = columnValue;
            c.timestamp = timeStamp;
            c.ttl       = 0;
            ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
            cosc.column = c;
            return cosc;
        }

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.fillValue    = conf.get("cassandra.fill_value");
            this.keyField     = Integer.parseInt(conf.get("cassandra.row_key_field"));
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job                    = new Job(getConf()); 
        job.setJarByClass(CassandraColumnLoader.class);
        job.setJobName("CassandraColumnLoader");
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
        ToolRunner.run(new Configuration(), new CassandraColumnLoader(), args);
        System.exit(0);
    }
}
