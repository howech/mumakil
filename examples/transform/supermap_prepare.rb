#!/usr/bin/env ruby

require 'rubygems'
require 'wukong'
require 'json'

#
# Usage:
#
# ./supermap_prepare.rb --run /path/to/input /path/to/output
#

#
# Simple wukong example map-reduce program to transform tab separated data of the form:
#
# (row_key, super_col_name, col_name, col_value)
#
# into that required for supermap insertion. After running this on your input data it
# should be ready to load directly using mumakil
#

class Reducer < Wukong::Streamer::AccumulatingReducer
  def start! row_key, super_col_name, col_name, col_value
    @hashmap = Hash.new{|h,k| h[k] = {} }
  end

  def accumulate row_key, super_col_name, col_name, col_value
    # {'super_col_name' => {'col_name' => 'col_value'}, ... }
    @hashmap[super_col_name][col_name] = col_value
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
