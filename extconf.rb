class Main
  $VERBOSE = nil
  #ENV["RUBYOPT"] = "-W0"

  require "rbconfig"
  require "fileutils"
  include FileUtils

  begin
    require 'rubygems'
  rescue Object
    nil
  end

  Makefile = <<-txt
    all:
    \t@ruby -e 42
    clean:
    \t@ruby -e 42
    install:
    \t@ruby -e 42
  txt

  def initialize 
    setup
    gen_makefile
    install_private_posixlock unless installed_posixlock?
    install_private_sqlite unless installed_sqlite?
    install_private_sqlite_ruby unless installed_sqlite_ruby?
  end

  def setup
    @config = Config::CONFIG

    @prefix = File.dirname(File.expand_path(__FILE__))

    Dir.chdir @prefix
    puts "-->> #{ Dir.pwd }"

    @libdir = File.join @prefix, "lib"
    @alldir = File.join @prefix, "all"
    @srcdir = File.join @alldir, "packages"

    @rqlibdir = Dir["#{ @libdir }/rq*"].detect{|e| test ?d, e}


    @local = File.join @rqlibdir, "local"
    @arch = @config["sitearch"] || @config["arch"] 
    @archdir = File.join @rqlibdir, @arch

    FileUtils.mkdir_p @local
    FileUtils.mkdir_p @archdir

    @ld_library_path = File.join @local, "lib"
    @ld_run_path = File.join @local, "lib"

    #ENV["LD_LIBRARY_PATH"] = "" #@ld_library_path
    ENV["LD_RUN_PATH"] = ""

    @srcs = Dir["#{ @srcdir }/*"]

    @ruby = File::join(@config['bindir'], @config['ruby_install_name']) << @config['EXEEXT']
  end

  def gen_makefile
    indent = nil
    open("Makefile", "w") do |f|
      Makefile.each do |line|
        indent ||= line[%r/^\s*/]
        line[%r/^#{ indent }/] = ''
        f.puts line
      end
    end
  end

  def install_private_posixlock
    tgz = @srcs.detect{|tgz| tgz =~ %r/(posixlock)-(\d+\.\d+\.\d+)\.(tar\.gz|tgz)$/}
    abort "no posixlock" unless tgz
    base, version, ext = $1, $2, $3
    tgz = File.expand_path tgz 
    mkdir_p "build"
    Dir.chdir "build" do
      puts "-->> #{ Dir.pwd }"
      cp tgz, "."
      tgz = File.basename tgz
      spawn "gzip -f -d #{ tgz }"
      spawn "tar xf #{ base }-#{ version }*tar"
      Dir.chdir "#{ base }-#{ version }" do
        puts "-->> #{ Dir.pwd }"
        spawn "#{ @ruby } extconf.rb"
        spawn "make clean"
        spawn "make"
        so = Dir["posixlock.{bundle,so}"].first
        chmod 0755, so 
        mv so, @archdir
        puts "#{ so } => #{ @archdir }"
      end
    end
  end

  def installed_posixlock?
    gem_or_pkg_installed?('posixlock')
  end

  def install_private_sqlite
    tgz = @srcs.detect{|tgz| tgz =~ %r/(sqlite)-(\d+\.\d+\.\d+)\.(tar\.gz|tgz)$/}
    abort "no sqlite" unless tgz
    base, version, ext = $1, $2, $3
    tgz = File.expand_path tgz 
    mkdir_p "build"
    Dir.chdir "build" do
      puts "-->> #{ Dir.pwd }"
      cp tgz, "."
      tgz = File.basename tgz
      spawn "gzip -f -d #{ tgz }"
      spawn "tar xf #{ base }-#{ version }*tar"
      Dir.chdir "#{ base }-#{ version }" do
        puts "-->> #{ Dir.pwd }"
        spawn "ls Makefile >/dev/null 2>&1 || ./configure --prefix=#{ @local }"
        spawn "chmod 777 ./libtool"
        spawn "make clean"
        spawn "make"
        spawn "make install"
      end
    end
  end

  def installed_sqlite?
    program_installed?('sqlite')
  end

  def install_private_sqlite_ruby
    tgz = @srcs.detect{|tgz| tgz =~ %r/(sqlite-ruby)-(\d+\.\d+\.\d+)\.(tar\.gz|tgz)$/}
    abort "no sqlite-ruby" unless tgz
    base, version, ext = $1, $2, $3
    tgz = File.expand_path tgz 
    mkdir_p "build"
    Dir.chdir "build" do
      puts "-->> #{ Dir.pwd }"
      cp tgz, "."
      tgz = File.basename tgz
      spawn "gzip -f -d #{ tgz }"
      spawn "tar xf #{ base }-#{ version }*tar"
      Dir.chdir "#{ base }-#{ version }" do
        puts "-->> #{ Dir.pwd }"
        Dir.chdir "ext" do
          puts "-->> #{ Dir.pwd }"
          spawn "#{ @ruby } extconf.rb --with-sqlite-dir=#{ @local }"
          spawn "make clean"
          spawn "make"
          so = Dir["_sqlite.*"].first
          chmod 0755, so 
          mv so, @archdir
          puts "#{ so } => #{ @archdir }"
        end

        Dir.chdir "lib" do
          Dir["*"].each do |e|
            chmod 0644, e 
            mv e, @rqlibdir
            puts "#{ e } => #{ @rqlibdir }"
          end
        end
      end
    end
  end

  def installed_sqlite_ruby?
    gem_or_pkg_installed?('sqlite')
  end

  def install_gem gem, opts = ''
    spawn "gem install #{ gem } #{ opts }" unless gem_or_pkg_installed?(gem)
  end

  def gem_or_pkg_installed? gem
    begin
      require gem
      gem
    rescue
      false
    end
  end

  def program_installed? basename
    dirname = ENV['PATH'].split(File::PATH_SEPARATOR).detect{|path| test(?x, File.join(path, basename.to_s))}
    dirname ? File.join(dirname, basename) : false
  end

  def spawn cmd
    puts cmd
    system cmd
  ensure
    abort "cmd <#{ cmd }> failed with <#{ $?.inspect }>" unless $?.exitstatus == 0
  end
end

Main.new
