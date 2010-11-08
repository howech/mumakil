require 'rubygems'
require 'wukong'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

module CassandraBulkLoader
  autoload TableLoader,  'cassandra_bulk_loader/table_loader'
  autoload ColumnLoader, 'cassandra_bulk_loader/column_loader'
end

