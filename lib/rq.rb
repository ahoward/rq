unless defined? $__rq__
  module RQ 
#--{{{
    require 'rbconfig'

    C = Config::CONFIG

    AUTHOR = 'ara.t.howard@gmail.com'
    LIBNAME = 'rq'
    VERSION = '3.5.0'
    LIBVER = "#{ LIBNAME }-#{ VERSION }"
    DIRNAME = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR
    ROOTDIR = File::dirname(DIRNAME)
    #LIBDIR = File::join(DIRNAME, LIBVER) + File::SEPARATOR
    LIBDIR = File::join(DIRNAME, LIBNAME) + File::SEPARATOR
    LOCALDIR = File::join(LIBDIR, 'local') + File::SEPARATOR
    LOCALBINDIR = File::join(LOCALDIR, 'bin') + File::SEPARATOR
    LOCALLIBDIR = File::join(LOCALDIR, 'lib') + File::SEPARATOR
    ARCH = C['sitearch'] || C['arch']
    ARCHLIBDIR = File::join(LIBDIR, ARCH) + File::SEPARATOR
    EXIT_SUCCESS = 0
    EXIT_FAILURE = 1
  #
  # builtin
  #
    require 'optparse'
    require 'logger'
    require 'socket'
    require 'optparse'
    require 'logger'
    require 'yaml'
    require 'yaml/store'
    require 'pp'
    require 'socket'
    require 'pathname'
    require 'tempfile'
    require 'fileutils'
    require 'tmpdir'
    require 'drb/drb'
    require 'digest/md5'
  #
  # try to load rubygems
  #
    begin
      require 'rubygems'
    rescue LoadError
      nil
    end

  #
  # depends - http://raa.ruby-lang.org
  #
    $:.push LIBDIR
    $:.push ARCHLIBDIR
    begin
      require 'arrayfields'
    rescue LoadError
      begin
        require LIBDIR + 'arrayfields'
      rescue LoadError
        abort "require arrayfields - http://raa.ruby-lang.org/project/arrayfields/"
      end
    end

    begin
      require 'lockfile'
    rescue LoadError
      begin
        require LIBDIR + 'lockfile'
      rescue LoadError
        abort "require lockfile - http://raa.ruby-lang.org/project/lockfile/"
      end
    end

    begin
      require 'posixlock'
    rescue LoadError
      begin
        require ARCHLIBDIR + 'posixlock'
      rescue LoadError
        abort "require posixlock - http://raa.ruby-lang.org/project/posixlock/"
      end
    end

  #
  # setup local require/lib/path/environment
  #
    $VERBOSE = nil
    ENV['PATH'] = [LOCALBINDIR, ENV['PATH']].join(':')
    ENV['LD_LIBRARY_PATH'] = [LOCALLIBDIR, ENV['LD_LIBRARY_PATH']].join(':')
    begin
      $:.unshift ARCHLIBDIR 
      $:.unshift LIBDIR  
      require 'sqlite'
    rescue LoadError
      abort "require sqlite - http://raa.ruby-lang.org/project/sqlite-ruby/"
    ensure
      $:.shift
      $:.shift
    end
    #system "ldd #{ ARCHLIBDIR }/_sqlite.so"
    #system "which sqlite"

  #
  # rq support libs
  #
    require LIBDIR + 'util'
    require LIBDIR + 'logging'
    require LIBDIR + 'orderedhash'
    require LIBDIR + 'orderedautohash'
    require LIBDIR + 'sleepcycle'
    require LIBDIR + 'qdb'
    require LIBDIR + 'jobqueue'
    require LIBDIR + 'job'
    require LIBDIR + 'jobrunner'
    require LIBDIR + 'jobrunnerdaemon'
    require LIBDIR + 'usage'
    require LIBDIR + 'mainhelper'
    require LIBDIR + 'creator'
    require LIBDIR + 'submitter'
    require LIBDIR + 'resubmitter'
    require LIBDIR + 'lister'
    require LIBDIR + 'statuslister'
    require LIBDIR + 'deleter'
    require LIBDIR + 'updater'
    require LIBDIR + 'querier'
    require LIBDIR + 'executor'
    require LIBDIR + 'configurator'
    require LIBDIR + 'snapshotter'
    require LIBDIR + 'locker'
    require LIBDIR + 'backer'
    require LIBDIR + 'rotater'
    require LIBDIR + 'feeder'
    require LIBDIR + 'recoverer'
    require LIBDIR + 'ioviewer'
    require LIBDIR + 'toucher'
    #require LIBDIR + 'resourcemanager'
    #require LIBDIR + 'resource'
    require LIBDIR + 'cron'
    require LIBDIR + 'rails'

#--}}}
  end # module rq
  $__rq__ = __FILE__ 
end
