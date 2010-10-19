(( Installer = Object.new )).instance_eval do

  require 'yaml'
  require 'rbconfig'
  require 'fileutils'
  $VERBOSE = nil

  @prefix = ARGV.shift or abort "no prefix"

  @this = File.expand_path __FILE__
  @dirname = File.dirname @this
  @packages = File.join @dirname, 'packages'
  @build = File.join @dirname, 'build'

  @prefix = File.expand_path @prefix 
  @arch = Config::CONFIG['arch'] 
  @archbindir = File.join @prefix, 'bin', @arch 
  @archlibdir = File.join @prefix, 'lib', @arch 
  @bindir = File.join @prefix, 'bin'
  @libdir = File.join @prefix, 'lib'

  def run 
    y config
    puts
    puts 'right?'
    gets

    Dir.chdir @dirname
    FileUtils.rm_rf @build
    FileUtils.mkdir_p @build
    FileUtils.mkdir_p @libdir
    FileUtils.mkdir_p @archlibdir
    FileUtils.mkdir_p @bindir
    FileUtils.mkdir_p @archbindir

    ENV['LD_LIBRARAY_PATH'] = [ @archlibdir, @libdir, ENV['LD_LIBRARAY_PATH'] ].flatten.compact.join ':'
    ENV['LD_RUN_PATH'] = [ @archlibdir, @libdir, ENV['LD_RUN_PATH'] ].flatten.compact.join ':'
    ENV['PATH'] = [ @archbindir, @bindir, ENV['PATH'] ].flatten.compact.join ':'

    Dir.chdir @build do
      #install_arrayfields
      #install_lockfile
      #install_main
      #install_open4
      #install_posixlock
      install_sqlite
    end
  end

  def config
    h = Hash.new
    instance_variables.each do |ivar|
      h[ivar[1..-1]] = instance_variable_get(ivar)
    end
    h
  end

  def install_arrayfields
    spawn "tar xvfz #{ @packages }/arrayfields*tgz >/dev/null 2>&1"
    Dir["./arrayfields*/lib/arrayfields.rb"].each do |entry|
      FileUtils.cp_r entry, @libdir
      puts "#{ entry } -->> #{ @libdir }"
    end
  end

  def install_lockfile
    spawn "tar xvfz #{ @packages }/lockfile*tgz >/dev/null 2>&1"
    Dir["./lockfile*/lib/lockfile.rb"].each do |entry|
      FileUtils.cp_r entry, @libdir
      puts "#{ entry } -->> #{ @libdir }"
    end
  end

  def install_main
    spawn "tar xvfz #{ @packages }/main*tgz >/dev/null 2>&1"
    Dir["./main*/lib/main*"].each do |entry|
      FileUtils.cp_r entry, @libdir
      puts "#{ entry } -->> #{ @libdir }"
    end
  end

  def install_open4
    spawn "tar xvfz #{ @packages }/open4*tgz >/dev/null 2>&1"
    Dir["./open4*/lib/open4.rb"].each do |entry|
      FileUtils.cp_r entry, @libdir
      puts "#{ entry } -->> #{ @libdir }"
    end
  end

  def install_posixlock
    spawn "tar xvfz #{ @packages }/posixlock*tgz >/dev/null 2>&1"
    Dir.chdir Dir["./posixlock*"].first do
      spawn "ruby extconf.rb && make >/dev/null 2>&1"
      Dir["./*.{so,bundle,dll}"].each do |entry|
        FileUtils.cp_r entry, @archlibdir
        puts "#{ entry } -->> #{ @archlibdir }"
      end
    end
  end

  def install_sqlite
    spawn "tar xvfz #{ @packages }/sqlite-2*gz"
    Dir.chdir Dir["./sqlite-2*"].first do
      spawn "./configure --prefix=`pwd`/prefix/ && make && make install >/dev/null 2>&1"
      #FileUtils.cp_r "./prefix/lib/libsqlite.a" 
      #puts "#{ entry } -->> #{ @archlibdir }"
    end
  end

  def spawn cmd
    system cmd or abort "cmd <#{ cmd }> failed"
  end

  def filelist glob
    Dir.glob(glob).map{|path| File.expand_path path}.sort.uniq
  end

end

Installer.run
