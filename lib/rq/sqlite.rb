# --------------------------------------------------------------------------
# sqlite.rb -- ruby interface for enhancing the core SQLite routines
# Copyright (C) 2003 Jamis Buck (jgb3@email.byu.edu)
# --------------------------------------------------------------------------
# This file is part of the SQLite ruby interface.
# 
# The SQLite/Ruby Interface is free software; you can redistribute it and/or
# modify  it  under the terms of the GNU General Public License as published
# by  the  Free  Software  Foundation;  either  version 2 of the License, or
# (at your option) any later version.
# 
# The SQLite/Ruby Interface is distributed in the hope that it will be useful,
# but   WITHOUT   ANY   WARRANTY;  without  even  the  implied  warranty  of
# MERCHANTABILITY  or  FITNESS  FOR  A  PARTICULAR  PURPOSE.   See  the  GNU
# General Public License for more details.
# 
# You  should  have  received  a  copy  of  the  GNU  General Public License
# along with the SQLite/Ruby Interface;  if  not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
# --------------------------------------------------------------------------
# This defines further enhancements to the SQLite::Database class.
#
# Author: Jamis Buck (jgb3@email.byu.edu)
# Date: June 2003
# --------------------------------------------------------------------------

require '_sqlite'
require 'time'
require 'base64'

# The SQLite module defines the classes and objects needed to interface with a
# SQLite database.
module SQLite

  # The Database class represents a single SQLite database.
  class Database

    # An alias for #new, with the exception that the mode parameter is optional and
    # defaults to 0 if unspecified.
    def self.open( name, mode=0 )
      new( name, mode )
    end

    # Returns a string with special characters escaped, such that the string may
    # be safely used as a string literal in an SQL query.
    def self.quote( string )
      return string.gsub( /'/, "''" )
    end

    # Returns a string that represents the serialization of the given object. The
    # string may safely be used in an SQL statement.
    def self.encode( object )
      Base64.encode64( Marshal.dump( object ) ).strip
    end

    # Returns an object that was serialized in the given string.
    def self.decode( string )
      Marshal.load( Base64.decode64( string ) )
    end

    # This is a convenience method for querying the database. If the (optional)
    # block is not specified, then an array of rows will be returned. Otherwise,
    # the given block will be executed one for each row in the result set.
    def execute( sql, arg=nil, &block )
      if block_given?
        exec( sql, block, arg )
      else
        rows = []
        exec( sql, proc { |row| rows.push row }, arg )
        return rows
      end
    end

    # A convenience method for retrieving the first row of the result set returned
    # by the given query.
    def get_first_row( sql )
      result = nil
      execute( sql ) do |row|
        result = row
        SQLite::ABORT
      end
      result
    end

    # A convenience method for retrieving the first column of the first row of the
    # result set returned by the given query.
    def get_first_value( sql )
      result = nil
      execute( sql ) do |row|
        result = row[0]
        SQLite::ABORT
      end
      result
    end

    # Performs an integrity check on the database. If there is a problem, it will raise an
    # exception, otherwise the database is fine.
    def integrity_check
      execute( "PRAGMA integrity_check" ) do |row|
        raise DatabaseException, row[0] if row[0] != "ok"
      end
    end

    # Defines a getter and setter for the named boolean pragma. A boolean pragma
    # is one that is either true or false.
    def self.define_boolean_pragma( name )
      class_eval <<-EODEF
        def #{name}
          get_first_value( "PRAGMA #{name}" ) != "0"
        end

        def #{name}=( mode )
          execute( "PRAGMA #{name}=\#{fix_pragma_parm(mode)}" )
        end
      EODEF
    end

    # Defines a method for invoking the named query pragma, with the given parameters.
    # A query pragma is one that accepts an optional callback block and invokes it for
    # each row that the pragma returns.
    def self.define_query_pragma( name, *parms )
      if parms.empty?
        definition = <<-EODEF
          def #{name}( &block )
            execute( "PRAGMA #{name}", &block )
          end
        EODEF
      else
        definition = <<-EODEF
          def #{name}( #{parms.join(',')}, &block )
            execute( "PRAGMA #{name}( '\#{#{parms.join("}','\#{")}}' )", &block )
          end
        EODEF
      end

      class_eval definition
    end

    # Defines a getter and setter for an enumeration pragma, which is like the boolean pragma
    # except that it accepts a range of discrete values.
    def self.define_enum_pragma( name, *enums )
      cases = ""
      enums.each do |enum|
        cases << "when \"" <<
                 enum.map { |i| i.to_s.downcase }.join( '", "' ) <<
                 "\": mode = \"" <<
                 enum.first.upcase << "\"\n"
      end

      class_eval <<-EODEF
        def #{name}
          get_first_value( "PRAGMA #{name}" )
        end

        def #{name}=( mode )
          case mode.to_s.downcase
            #{cases}
            else
              raise DatabaseException, "unrecognized #{name} '\#{mode}'"
          end

          execute( "PRAGMA #{name}='\#{mode}'" )
        end
      EODEF
    end

    # Defines a getter and setter for a pragma that accepts (or returns) an integer pragma.
    def self.define_int_pragma( name, *enums )
      class_eval <<-EODEF
        def #{name}
          get_first_value( "PRAGMA #{name}" ).to_i
        end

        def #{name}=( value )
          execute( "PRAGMA #{name}=\#{value.to_i}" )
        end
      EODEF
    end

    # An internal method for converting the pragma parameter of the boolean pragmas to
    # something that SQLite can understand.
    def fix_pragma_parm( parm )
      case parm
        when String
          case parm.downcase
            when "on", "yes", "true", "y", "t": return "'ON'"
            when "off", "no", "false", "n", "f": return "'OFF'"
            else
              raise DatabaseException, "unrecognized pragma parameter '#{parm}'"
          end
        when true, 1
          return "ON"
        when false, 0, nil
          return "OFF"
        else
          raise DatabaseException, "unrecognized pragma parameter '#{parm.inspect}'"
      end
    end
    private :fix_pragma_parm

    define_int_pragma "cache_size"
    define_int_pragma "default_cache_size"

    define_enum_pragma "default_synchronous", [ 'full', 2 ], [ 'normal', 1 ], [ 'off', 0 ]
    define_enum_pragma "default_temp_store", [ 'default', 0 ], [ 'file', 1 ], [ 'memory', 2 ]
    define_enum_pragma "synchronous", [ 'full', 2 ], [ 'normal', 1 ], [ 'off', 0 ]
    define_enum_pragma "temp_store", [ 'default', 0 ], [ 'file', 1 ], [ 'memory', 2 ]

    define_boolean_pragma "empty_result_callbacks"
    define_boolean_pragma "full_column_names"
    define_boolean_pragma "parser_trace"
    define_boolean_pragma "show_datatypes"
    define_boolean_pragma "vdbe_trace"

    define_query_pragma "database_list"
    define_query_pragma "foreign_key_list", "table_name"
    define_query_pragma "index_info", "index"
    define_query_pragma "index_list", "table"
    define_query_pragma "table_info", "table"
  end

  # The TypeTranslator is a singleton class that manages the routines that have
  # been registered to convert particular types. The translator only manages
  # conversions in queries (where data is coming out of the database), and not
  # updates (where data is going into the database).
  class TypeTranslator
    @@default_translator = proc { |type,value| value }
    @@translators = Hash.new( @@default_translator )

    # Registers the given block to be used when a value of the given type needs
    # to be translated from a string.
    def self.add_translator( type, &block )
      @@translators[ type_name( type ) ] = block
    end

    # Looks up the translator for the given type, and asks it to convert the
    # given value.
    def self.translate( type, value )
      unless value.nil?
        @@translators[ type_name( type ) ].call( type, value )
      end
    end

    # Finds the base type name for the given type. Type names with parenthesis
    # (like "VARCHAR(x)" and "DECIMAL(x,y)") will have the parenthesized portion
    # removed.
    def self.type_name( type )
      type = $1 if type =~ /^(.*?)\(/
      type.upcase
    end
  end

  [ "date",
    "datetime",
    "time" ].each { |type| TypeTranslator.add_translator( type ) { |t,v| Time.parse( v ) } }

  [ "decimal",
    "float",
    "numeric",
    "double",
    "real",
    "dec",
    "fixed" ].each { |type| TypeTranslator.add_translator( type ) { |t,v| v.to_f } }

  [ "integer",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint" ].each { |type| TypeTranslator.add_translator( type ) { |t,v| v.to_i } }

  [ "bit",
    "bool",
    "boolean" ].each do |type|
    TypeTranslator.add_translator( type ) do |t,v|
      !( v.to_i == 0 ||
         v.downcase == "false" ||
         v.downcase == "f" ||
         v.downcase == "no" ||
         v.downcase == "n" )
    end
  end

  TypeTranslator.add_translator( "timestamp" ) { |type, value| Time.at( value.to_i ) }
  TypeTranslator.add_translator( "tinyint" ) do |type, value|
    if type =~ /\(\s*1\s*\)/
      value.to_i == 1
    else
      value.to_i
    end
  end

end
