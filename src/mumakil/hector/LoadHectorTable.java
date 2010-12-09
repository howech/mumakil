/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mumakil.hector;

import java.util.Scanner;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 *
 * @author chris
 */
public class LoadHectorTable {
     /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws HectorException {
        int keyfield = 0;
        String hosts = "localhost";
        int port = 9160;
        String clusterName = "Test Cluster";
        String keyspaceName = "myKeySpace";
        String columnFamily = "colFam";
        String[] fieldNames = { "key", "value" };
        boolean verbose = false;

        // TODO: find a decent option parse library for java
        for(int i=0;i<args.length;++i) {
            if( "--hosts".equals(args[i]) ) {
                ++i;
                hosts = args[i];
            } else if ( "--port".equals(args[i])) {
                ++i;
                port = Integer.parseInt( args[i] );
            } else if ( "--keyfield".equals(args[i])) {
                ++i;
                keyfield = Integer.parseInt( args[i]);
            } else if ( "--keyspace".equals(args[i])) {
                ++i;
                keyspaceName = args[i];
            } else if ( "--cluster".equals(args[i])) {
                ++i;
                clusterName = args[i];
            } else if ( "--columnFamily".equals(args[i])) {
                ++i;
                columnFamily = args[i];
            } else if ( "--fieldnames".equals(args[i])) {
                ++i;
                fieldNames = args[i].split(",");
            } else if ( "--help".equals(args[i])) {
                System.out.println("Usage: java LoadTable [args]\n\t--hosts host1,host2,...\n\t--port <n>[default 9160]");
                System.out.println("\t--keyfield n - 0 based index of the column to use as the key");
                System.out.println("\t--keyspace ks - the name of the keyspace to load into");
                System.out.println("\t--cluster name - the name of the cluster to load into");
                System.out.println("\t--columnFamily cfname - the name of the column family to load into");
                System.out.println("\t--fieldnames name1,name2,-,---namex  - Column names for the corresponding fields. The special name '-' means to skip the column.");
                System.exit(-1);
            } else if ("--verbose".equals(args[i])) {
                verbose = true;
            }
        }

        if( verbose ) {
            System.out.printf("hosts:         %s\n",hosts);
            System.out.printf("port:          %d\n",port);
            System.out.printf("cluster:       %s\n",clusterName);
            System.out.printf("keyspace:      %s\n",keyspaceName);
            System.out.printf("column family: %s\n",columnFamily);
            System.out.printf("key field:     %d\n",keyfield);
            System.out.print("field names:    ");
            for( String fn : fieldNames ) {
                System.out.printf(" %s",fn);
            }
            System.out.println();
        }

        CassandraHostConfigurator x = new CassandraHostConfigurator();
        x.setHosts(hosts);  // Note: although docs indicate that comma separated works here, I have found that it doesnt work well.
        x.setPort(port);
        x.setAutoDiscoverHosts(true);

        Cluster cluster = HFactory.getOrCreateCluster(clusterName, x);
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);

        ConsistencyLevelPolicy allOne = new AllOneConsistencyLevelPolicy();
        keyspace.setConsistencyLevelPolicy( allOne  );

        AbstractSerializer ss = StringSerializer.get(); // Everything is a string.
        Mutator m = HFactory.createMutator( keyspace, ss );

        Scanner lines = new Scanner( System.in );

        String line;
        HColumn<String, String> column;
        MutationResult result;

        int ii = 0;
        while( lines.hasNextLine() ) {
            // Give some indication that we are alive
            if(ii%1000 == 0)
                System.out.println(ii);
            ii++;

            // Split input lines on tabs - sorry, no quoting.
            String[] fields = lines.nextLine().split("\\t+");
            if( keyfield < fields.length && fields[keyfield].length() > 0 ) {
                for(int i=0;i<fields.length && i<fieldNames.length;++i) {
                    if( "-".equals( fieldNames[i] ) )
                        continue; // skip "-" fieldnames

                    column = HFactory.createColumn( fieldNames[i], fields[i] , ss, ss);
                    m.addInsertion(fields[keyfield], columnFamily, column);
                }
            }

            if(ii%100 == 0) // TODO: parameterize the commit block size
                m.execute();
        }

        m.execute(); // Make sure that we write out anything remaining
        cluster.getConnectionManager().shutdown(); // close down the connection
    }
}
