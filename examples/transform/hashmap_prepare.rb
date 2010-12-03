#!/usr/bin/env ruby

require 'rubygems'
require 'wukong'
require 'json'

#
# Usage:
#
# ./hashmap_prepare.rb --run /path/to/input /path/to/output
#

#
# Simple wukong example map-reduce program to transform tab separated data of the form:
#
# (row_key, col_name, col_value)
#
# into that required for map insertion. After running this on your input data it
# should be ready to load directly using mumakil
#

class Reducer < Wukong::Streamer::AccumulatingReducer
  def start! row_key, col_name, col_value
    @hashmap = {}
  end

  def accumulate row_key, col_name, col_value
    @hashmap[col_name] = col_value
  end

  def finalize
    yield [key, @hashmap.to_json]
  end
end

Wukong::Script.new(
  nil,
  Reducer,
  :map_command => '/bin/cat'
  ).run
