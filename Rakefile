#!/usr/bin/env ruby

require 'rubygems'
require 'wukong'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

Settings.define :src,            :default => "src",                 :description => "Java source file to compile"
Settings.define :main_class,     :default => "CassandraBulkLoader", :description => "Main java class to run"
Settings.define :target,         :default => "build",               :description => "Build target, this is where compiled classes live"
Settings.define :hadoop_home,    :env_var => "HADOOP_HOME",    :default => "/usr/lib/hadoop",                   :description => "Path to hadoop installation"
Settings.define :cassandra_home, :env_var => "CASSANDRA_HOME", :default => "/home/jacob/Programming/cassandra", :description => "Path to cassandra installation"

Settings.resolve!
options = Settings.dup

#
# Returns full classpath
#
def classpath options
  cp = ["#{options.cassandra_home}/build/classes"]
  Dir[
    "#{options.hadoop_home}/hadoop*.jar",
    "#{options.hadoop_home}/lib/*.jar",
    "#{options.cassandra_home}/lib/*.jar"
  ].each{|jar| cp << jar}
  cp.join(':')
end

#
# Returns whitespace separated list of java source files to compile
#
def srcs options
  sources = Dir["#{options.src}/*.java"].inject([]){|sources, src| sources << src; sources}
  sources.join(' ')
end

#
# FIXME: Needs to be idempotent ...
#
task :compile do
  puts "Compiling #{options.src} ..."
  snakeized = options.main_class.underscore
  mkdir_p File.join(options.target, snakeized)
  system "javac -cp #{classpath(options)} -d #{options.target}/#{snakeized} #{srcs(options)}"
  system "jar -cvf  #{options.target}/#{snakeized}.jar -C #{options.target}/#{snakeized} . "
end

task :default => [:compile]
