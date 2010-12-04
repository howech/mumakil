import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.avro.AvroRecordFactory;
import org.apache.cassandra.avro.Column;
import org.apache.cassandra.avro.SuperColumn;
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

public class LoadSuperColumnMap extends Configured implements Tool {
    
    public static class ColumnFamilyMapper extends Mapper<LongWritable, Text, ByteBuffer, List<Mutation>> {
        
        private List<Mutation> rowMutationList = new ArrayList<Mutation>();

        private Integer longNames;
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields      = value.toString().split("\t");
            ByteBuffer rowKey    = ByteBuffer.wrap(fields[0].getBytes());
            if(longNames == 1) {
                byte[] superColName = stringToLongBytes(fields[1]);
                for(int i = 2; i < fields.length-1; i += 2) {
                    try {
                        rowMutationList.add(getSuperMutation(superColName, stringToLongBytes(fields[i]), fields[i+1].getBytes(), System.currentTimeMillis() * 1000));
                    } catch (NumberFormatException e) {
                        return;
                    }
                    context.write(rowKey, rowMutationList);
                    rowMutationList.clear();
                }
            } else {
                byte[] superColName  = fields[1].getBytes();
                for(int i = 2; i < fields.length-1; i += 2) {
                    rowMutationList.add(getSuperMutation(superColName, fields[i].getBytes(), fields[i+1].getBytes(), System.currentTimeMillis() * 1000));
                    context.write(rowKey, rowMutationList);
                    rowMutationList.clear();
                }
            }
        }

        private static byte[] stringToLongBytes(String value) {
            Long longValue = Long.parseLong(value);
            byte[] asBytes = new byte[ 8 ];
            asBytes[0] = (byte)(longValue >>> 56);
            asBytes[1] = (byte)(longValue >>> 48);
            asBytes[2] = (byte)(longValue >>> 40);
            asBytes[3] = (byte)(longValue >>> 32);
            asBytes[4] = (byte)(longValue >>> 24);
            asBytes[5] = (byte)(longValue >>> 16);
            asBytes[6] = (byte)(longValue >>>  8);
            asBytes[7] = (byte)(longValue >>>  0);
            return asBytes;
        }
        
        private static Mutation getSuperMutation(byte[] supername, byte[] name, byte[] value, Long timeStamp) {
            Mutation m = new Mutation();
            m.column_or_supercolumn = getSC(supername, name, value, timeStamp);
            return m;
        }

        private static ColumnOrSuperColumn getSC(byte[] supername, byte[] name, byte[] value, Long timeStamp) {
            ByteBuffer columnName   = ByteBuffer.wrap(name);
            ByteBuffer superColName = ByteBuffer.wrap(supername);
            ByteBuffer columnValue  = ByteBuffer.wrap(value);

            List<Column> colList = new ArrayList<Column>();
            Column c       = new Column();
            c.name         = columnName;
            c.value        = columnValue;
            c.timestamp    = timeStamp;
            c.ttl          = 0;
            colList.add(c);
            SuperColumn sc = AvroRecordFactory.newSuperColumn(superColName, colList);
            ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
            cosc.super_column = sc;
            return cosc;
        }

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.longNames    = Integer.parseInt(conf.get("cassandra.longnames"));
        }
    }
    
    public int run(String[] args) throws Exception {
        Job job                    = new Job(getConf()); 
        job.setJarByClass(LoadSuperColumnMap.class);
        job.setJobName("LoadSuperColumnMap");
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
        ToolRunner.run(new Configuration(), new LoadSuperColumnMap(), args);
        System.exit(0);
    }
}
