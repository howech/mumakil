#!/usr/bin/env jruby

require 'java'
require 'rubygems'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)
require 'json'

#
# Make a yaml file on disk that looks like the keyspace you want. Take a look at the example.
#
# bin/schemer.rb --host=`hostname -i` --port=9160 mykeyspace.yaml
#

Settings.define :host, :default => 'localhost', :description => 'Host running cassandra thrift daemon'
Settings.define :port, :default => '9160',      :description => 'Port to interface with cassandra over'
Settings.define :cassandra_home, :env_var => 'CASSANDRA_HOME', :default => '/home/jacob/Programming/cassandra', :description => 'Path to cassandra installation'

Settings.resolve!
options = Settings.dup

Dir[
  "#{options.cassandra_home}/lib/*.jar",
].each{|jar| require jar}

java_import 'org.apache.thrift.TException'
java_import 'org.apache.thrift.protocol.TBinaryProtocol'
java_import 'org.apache.thrift.transport.TFramedTransport'
java_import 'org.apache.thrift.transport.TSocket'
java_import 'org.apache.thrift.transport.TTransport'
java_import 'org.apache.cassandra.thrift.Cassandra'
java_import 'org.apache.cassandra.thrift.KsDef'
java_import 'org.apache.cassandra.thrift.CfDef'

class CliClient
  attr_accessor :host, :port, :client, :keyspaces, :cluster_name
  def initialize host, port
    @host = host.to_s
    @port = port.to_i
    socket          = TSocket.new(@host, @port)
    transport       = TFramedTransport.new(socket)
    binary_protocol = TBinaryProtocol.new(transport, true, true)
    @client         = Cassandra::Client.new(binary_protocol)
    transport.open
    @keyspaces      = Hash.new{|h,k| h[k] = ""}
  end

  def cluster_name
    @cluster_name = client.describe_cluster_name
  end

  #
  # Fetch metadata about a given keyspace
  #
  def keyspace_meta_data keyspace
    return @keyspaces[keyspace] unless @keyspaces[keyspace].empty?
    begin
      ks_meta_obj = client.describe_keyspace(keyspace)
    rescue NativeException
      raise "Keyspace [#{keyspace}] does not exist."
    end
    ks_meta     = {}
    ks_meta[:replication_factor] = ks_meta_obj.replication_factor
    ks_meta[:column_families]    = []
    ks_meta_obj.cf_defs.each do |cf_def|
      cf = {}
      cf[:name]      = cf_def.name
      cf[:type]      = cf_def.column_type
      cf[:sorted_by] = cf_def.comparator_type
      ks_meta[:column_families] << cf
    end
    @keyspaces[keyspace] = ks_meta
    @keyspaces[keyspace]
  end

  #
  # Dump keyspace metadata as json to standard out
  #
  def dump_keyspace_json keyspace
    puts keyspace_meta_data(keyspace).to_json
  end

  #
  # Given a hash of metadata for desired keyspace we create it
  #
  def create_keyspace ks_hash
    return if keyspace_exists?(ks_hash[:name])
    mlist  = java.util.LinkedList.new
    ks_def                    = KsDef.new
    ks_def.name               = ks_hash[:name]
    ks_def.strategy_class     = ks_hash[:replica_placement_strategy]
    ks_def.replication_factor = ks_hash[:replication_factor]
    ks_def.cf_defs            = mlist
    client.system_add_keyspace(ks_def)
  end

  def create_column_family keyspace, cf_hash
    raise "Keyspace [#{keyspace}] does not exist." unless keyspace_exists?(keyspace)
    raise "Column family [#{cf_hash[:name]}] exists in keyspace [#{keyspace}]" if column_family_exists?(keyspace, cf_hash[:name])
    client.set_keyspace(keyspace)
    cf_def = CfDef.new(keyspace, cf_hash[:name])
    cf_def.set_column_type(cf_hash[:column_type])                    unless cf_hash[:column_type].nil?
    cf_def.set_comparator_type(cf_hash[:compare_with])               unless cf_hash[:compare_with].nil?
    cf_def.set_subcomparator_type(cf_hash[:compare_subcolumns_with]) unless cf_hash[:compare_subcolumns_with].nil?
    client.system_add_column_family(cf_def)
  end

  def create_keyspace_from_yaml schema_path
    schema = schema_from_yaml File.read(schema_path)
    create_keyspace schema
    schema[:column_families].each do |cf|
      create_column_family schema[:name], cf
    end
  end

  def schema_from_yaml yaml_schema
    schema = YAML.load(yaml_schema)
    schema = keys_to_symbols schema
    schema[:column_families].map! do |cf|
      keys_to_symbols cf
    end
    schema
  end

  #
  # Blargh.
  #
  def keys_to_symbols h
    h.inject({}){|x, (k,v)| x[k.to_sym] = v; x}
  end

  #
  # Need to examine the exception more closely to make sure but
  # this'll do for now
  #
  def keyspace_exists? keyspace
    begin
      client.describe_keyspace(keyspace)
    rescue NativeException
      return false
    end
    return true
  end

  #
  # Seriously.
  #
  def column_family_exists? keyspace, column_family
    return false unless keyspace_exists?(keyspace)
    client.set_keyspace(keyspace)
    column_families = keyspace_meta_data(keyspace)[:column_families]
    return false if column_families.empty?
    return false unless column_families.find{|cf| cf[:name] == column_family}
    true
  end

end

cli = CliClient.new(options.host, options.port)
cli.create_keyspace_from_yaml options.rest.first
