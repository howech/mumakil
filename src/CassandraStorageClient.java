import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.FBUtilities;

public class CassandraStorageClient {
  /*
   *  Shamelessy taken from AbstractCassandraDaemon.java, this just goes through
   *  the mundane day-to-day initialization of Cassandra crap.
   */
  public static void init() throws IOException {
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

  public static void close() {
    try {
      // Sleep just in case the number of keys we send over is small
      Thread.sleep(3*1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    StorageService.instance.stopClient();
  }
}
