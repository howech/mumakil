/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
// package mumakil.hector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.query.CountQuery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class LoadHectorColumns extends Configured implements Tool {

    enum CassandraRecords {
        LOADED_COLS,
        LOADED_ROWS,
        BAD
    }

    public static class ColumnFamilyMapper extends Mapper<LongWritable, Text, ByteBuffer, List<String>> {

        private Cluster  cluster;
        private Keyspace keyspace;
        private String   columnFamily;
        private String   fillValue;
        private Mutator  mutator;

        private int keyField;

        private boolean longNames;
        private boolean longVals;
        private boolean longKeys;
        
        private AbstractSerializer nameSerializer;
        private AbstractSerializer valueSerializer;
        private AbstractSerializer keySerializer;

        private Counter loadedRows;
        private Counter loadedColumns;
        private Counter badColumns;    

        @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String rowKey = fields[keyField];
            
            try {
                if ( longNames ) {
                    for (int i = 0; i < fields.length; i++) {
                        if (i != keyField) {
                            try {
                                HColumn hc = HFactory.createColumn(Long.parseLong(fields[i]), fillValue,
                                                                   nameSerializer, valueSerializer);
                                mutator.addInsertion(rowKey, columnFamily, hc);
                                loadedColumns.increment(1);
                            } catch (NumberFormatException e) {
                                badColumns.increment(1);
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < fields.length; i++) {
                        if (i != keyField) {
                            HColumn hc = HFactory.createColumn(fields[i], fillValue, nameSerializer, valueSerializer);
                            mutator.addInsertion(rowKey, columnFamily, hc);
                            loadedColumns.increment(1);                            
                        }
                    }
                }
            } finally {
                loadedRows.increment(1);
                mutator.execute();
            }
        }

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            CassandraHostConfigurator cassHostConfig = new CassandraHostConfigurator();
            cassHostConfig.setHosts(conf.get("cassandra.initial_host")); // takes a list of hosts
            cassHostConfig.setPort(Integer.parseInt(conf.get("cassandra.thrift_port")));
            cassHostConfig.setAutoDiscoverHosts(true);
            cluster  = HFactory.getOrCreateCluster("Magilla", cassHostConfig);
            keyspace = HFactory.createKeyspace(conf.get("cassandra.keyspace"), cluster );

            keySerializer = StringSerializer.get();
            valueSerializer = StringSerializer.get();
            
            longNames = "1".equals(conf.get("cassandra.longnames"));
            if( longNames ) {
                nameSerializer = LongSerializer.get();
            } else {
                nameSerializer = StringSerializer.get();
            }

            mutator      = HFactory.createMutator(keyspace, keySerializer);
            columnFamily = conf.get("cassandra.column_family");
            fillValue    = conf.get("cassandra.fill_value");
            keyField     = Integer.parseInt(conf.get("cassandra.row_key_field"));
            
            // Counters
            loadedRows    = context.getCounter(CassandraRecords.LOADED_ROWS);
            loadedColumns = context.getCounter(CassandraRecords.LOADED_COLS);
            badColumns    = context.getCounter(CassandraRecords.BAD);    
        }   
            
        @Override
        protected void cleanup(Mapper.Context context) {
            cluster.getConnectionManager().shutdown();
        }

    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(LoadHectorColumns.class);
        job.setJobName("LoadHectorColumns");
        job.setMapperClass(ColumnFamilyMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class); // don't write any output

        // Handle input path
        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));

        // Submit job to server and wait for completion
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new LoadHectorColumns(), args);
        System.exit(0);
    }    
}
