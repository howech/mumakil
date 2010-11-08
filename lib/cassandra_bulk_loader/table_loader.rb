module CassandraBulkLoader
  class TableLoader < BaseRunner

    def mainclass
      "CassandraTableLoader"
    end

    #
    # Launches the hadoop job to bulk load into cassandra
    #
    def loadtable cfname, hdfs_path, fieldnames

    end

  end
end
