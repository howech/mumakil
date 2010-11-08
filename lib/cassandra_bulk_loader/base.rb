module CassandraBulkLoader
  class BaseRunner
    attr_accessor :options

    def initialize options

    end

    #
    # Needs to change depending on whether you're loading tables, columns, etc
    #
    def mainclass
      raise "You must override this in your subclass"
    end

    def execute
      remove_nil_outfile
      hadoop_cmd = [
        "HADOOP_CLASSPATH=#{hadoop_classpath}",
        "#{options.hadoop_home}/bin/hadoop jar #{run_jar}",
        mainclass,
        "-Dcassandra.config=file://#{options.cassandra_config}",
        "-Dcassandra.keyspace=#{options.ks}",
        "-Dcassandra.column_family=#{options.cf}",
        "-Dcassandra.row_key_field=#{options.key_field}",
        "-Dcassandra.field_names=#{options.field_names}",
        timestamp,
        sub_key_field,
        "-Dmapred.min.split.size=#{options.min_split_size}",
        "-libjars #{libjars}",
        "#{options.rest.first}",
        "#{options.nil_outfile}"
      ].flatten.compact.join(" \t\\\n  ")
      system %Q{ echo #{hadoop_cmd} }
      system %Q{ #{hadoop_cmd} }
    end

    def remove_nil_outfile
      system %Q{ hdp-rm -r #{options.nil_outfile} }
    end

    def hadoop_classpath
      hdp_cp = []
      Dir[
        "#{options.cassandra_home}/lib/*cassandra*.jar"
      ].each{|jar| hdp_cp << jar}
      hdp_cp.join(':')
    end

    def run_jar
      File.dirname(File.expand_path(__FILE__))+'/../../build/cassandra_bulk_loader.jar'
    end

    def libjars
      libjars = []
      Dir[
        "#{options.cassandra_home}/lib/*.jar"
      ].each{|jar| libjars << jar}
      libjars.join(',')
    end

    def timestamp
      return unless options.ts_field
      "-Dcassandra.timestamp_field=#{options.ts_field}"
    end

    def sub_key_field
      return unless options.sub_key_field
      "-Dcassandra.sub_key_field=#{options.sub_key_field}"
    end

  end
end
