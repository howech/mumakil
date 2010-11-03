import java.io.IOException;
import java.lang.NumberFormatException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.net.InetAddress;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.TaskID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.db.clock.TimestampReconciler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.service.MigrationManager;

/*

  First of all we expect a comma separated list of field names to be passed in via '-Dcassandra.field_names=...'. With this
  we read in lines from the hdfs path expecting that each record adheres to this schema. This record is inserted directly
  into cassandra, no thrift.

 */
public class CassandraBulkLoader extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        
        private JobConf jobconf;
        private ColumnFamily columnFamily;
        private String keyspace;
        private String cfName;
        private Integer keyField;
        private Integer tsField;
        private String[] fieldNames;


        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            /* Split the line into fields */
            String[] fields = value.toString().split("\t");
            
            /* Handle custom timestamp for all columns */
            Long timeStamp;
            if (tsField == -1) {
                timeStamp = System.currentTimeMillis();
            } else {
                timeStamp = Long.parseLong(fields[tsField]);
            }

            /* Create linked list of column families, this will hold only one column family */
            List<ColumnFamily> columnFamilyList = new LinkedList<ColumnFamily>();
            columnFamily = ColumnFamily.create(keyspace, cfName);
            for(int i = 0; i < fields.length; i++) {
                if (i < fieldNames.length) {
                    columnFamily.addColumn(new QueryPath(cfName, null, fieldNames[i].getBytes("UTF-8")), fields[i].getBytes("UTF-8"), new TimestampClock(timeStamp));
                }
            }
            columnFamilyList.add(columnFamily);
            
            /* Serialize our data as a binary message and send it out to Cassandra endpoints */
            Message message = createMessage(keyspace, fields[keyField].getBytes("UTF-8"), cfName, columnFamilyList);
            List<IAsyncResult> results = new ArrayList<IAsyncResult>();
            for (InetAddress endpoint: StorageService.instance.getNaturalEndpoints(keyspace, fields[keyField].getBytes())) {
                results.add(MessagingService.instance.sendRR(message, endpoint));
            }
            
            // /* Wait for acks */
            // for (IAsyncResult result : results) {
            //     try {
            //         result.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            //     } catch (TimeoutException e) {
            //         /* We should retry here */
            //         throw new RuntimeException(e);
            //     }
            // }
        }

        /*
         *  Called at very beginning of map task, sets up basic configuration at the task level
         */
        public void configure(JobConf job) {
            
            this.jobconf      = job;
            this.keyspace     = job.get("cassandra.keyspace");
            this.cfName       = job.get("cassandra.column_family");
            this.fieldNames   = job.get("cassandra.field_names").split(",");
            this.keyField     = Integer.parseInt(job.get("cassandra.row_key_field"));

            /* Deal with custom timestamp field */
            try {
                this.tsField   = Integer.parseInt(job.get("cassandra.timestamp_field"));
            } catch (NumberFormatException e) {
                this.tsField = -1;
            }
            
            System.out.println("Using field ["+keyField+"] as row key");
            /* Set cassandra config file (cassandra.yaml) */
            System.setProperty("cassandra.config", job.get("cassandra.config"));
            
            try {
                init_cassandra();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(10*1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        /*
         *  Shamelessy taken from AbstractCassandraDaemon.java, this just goes through
         *  the mundane day-to-day initialization of Cassandra crap.
         */
        private void init_cassandra() throws IOException {
            FBUtilities.tryMlockall();
            Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    public void uncaughtException(Thread t, Throwable e) {
                        if (e instanceof OutOfMemoryError) {
                            System.exit(100);
                        }
                    }
                });
            for (CFMetaData cfm : DatabaseDescriptor.getTableMetaData(Table.SYSTEM_TABLE).values())
                ColumnFamilyStore.scrubDataDirectories(Table.SYSTEM_TABLE, cfm.cfName);
            try {
                SystemTable.checkHealth();
            } catch (ConfigurationException e) {
                System.exit(100);
            }
        
            // load keyspace descriptions.
            try {
                DatabaseDescriptor.loadSchemas();
            } catch (IOException e) {
                System.exit(100);
            }
        
            // clean up debris in the rest of the tables
            for (String table : DatabaseDescriptor.getTables()) {
                    for (CFMetaData cfm : DatabaseDescriptor.getTableMetaData(table).values()) {
                        ColumnFamilyStore.scrubDataDirectories(table, cfm.cfName);
                    }
            }

            // initialize keyspaces
            for (String table : DatabaseDescriptor.getTables()) {
                Table.open(table);
            }

            // replay the log if necessary and check for compaction candidates
            CommitLog.recover();
            CompactionManager.instance.checkAllColumnFamilies();
        
            UUID currentMigration = DatabaseDescriptor.getDefsVersion();
            UUID lastMigration = Migration.getLastMigrationId();
            if ((lastMigration != null) && (lastMigration.timestamp() > currentMigration.timestamp())) {
                MigrationManager.applyMigrations(currentMigration, lastMigration);
            }
        
            SystemTable.purgeIncompatibleHints();
            StorageService.instance.initClient(); // <-- Start a client, not server, so we don't try and keep data for ourselves.
        }

        /*
         *  Called at the end of the map task
         */
        public void close() {
            try {
                // Sleep just in case the number of keys we send over is small
                Thread.sleep(3*1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            StorageService.instance.stopClient();
        }
    }

    /*
     *  Create a special binary message just for Cassandra. How cute.
     */
    public static Message createMessage(String Keyspace, byte[] Key, String CFName, List<ColumnFamily> ColumnFamiles) {
        ColumnFamily baseColumnFamily;
        DataOutputBuffer bufOut = new DataOutputBuffer();
        RowMutation rm;
        Message message;
        Column column;
    
        /* Get the first column family from list, this is just to get past validation */
        baseColumnFamily = new ColumnFamily(ColumnFamilyType.Standard,
                                            ClockType.Timestamp,
                                            DatabaseDescriptor.getComparator(Keyspace, CFName),
                                            DatabaseDescriptor.getSubComparator(Keyspace, CFName),
                                            TimestampReconciler.instance,
                                            CFMetaData.getId(Keyspace, CFName));
        
        for(ColumnFamily cf : ColumnFamiles) {
            bufOut.reset();
            ColumnFamily.serializer().serializeWithIndexes(cf, bufOut);
            byte[] data = new byte[bufOut.getLength()];
            System.arraycopy(bufOut.getData(), 0, data, 0, bufOut.getLength());
            column = new Column(FBUtilities.toByteArray(cf.id()), data, new TimestampClock(0));
            baseColumnFamily.addColumn(column);
        }
        rm = new RowMutation(Keyspace, Key);
        rm.add(baseColumnFamily);
    
        try { /* Make message */
            message = rm.makeRowMutationMessage(StorageService.Verb.BINARY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return message;
    }

    /*
     *  Interprets commandline args, sets configuration, and actually runs the job
     */
    public int run(String[] args) {
        JobConf conf                = new JobConf(getConf(), CassandraBulkLoader.class);
        GenericOptionsParser parser = new GenericOptionsParser(conf,args);

        conf.setJobName("CassandraBulkLoader");
        conf.setMapperClass(Map.class);
        conf.setNumReduceTasks(0);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
	
        FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
        try {
            JobClient.runJob(conf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

   /*
    *  Main class, simply shells out to generic tool runner for Hadoop. This
    *  means you can pass command line script the usual options with '-D, -libjars'
    *  for free.
    */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CassandraBulkLoader(), args);
        System.exit(res);
    }
}
