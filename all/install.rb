class Installer
  $VERBOSE = nil
  getenv = lambda{|key| ENV[key] or abort("no ENV['#{ key }']")}
  PREFIX = getenv["prefix"]
  MAKE = getenv["make"] || getenv["MAKE"]
  PACKAGEDIR = getenv["packagedir"]
  BUILDDIR = getenv["builddir"]
  SUDO = getenv["sudo"]
  RUBY = getenv["ruby"]
  LDFLAGS = getenv["LDFLAGS"]
  CFLAGS = getenv["CFLAGS"]

  INSTALL = "#{ PACKAGEDIR }/INSTALL"

  def run
    manifest = test ?e, INSTALL 

    packages = 
      if manifest 
        lines = IO.readlines INSTALL
        lines.delete_if{|line| line.strip.empty? or line =~ %r/^\s*#/}
        lines.map{|line| File.join(PACKAGEDIR, File.basename(line.strip))}
      else
        filelist "#{ PACKAGEDIR }/*.{tar.gz,tgz}"
      end

    packages.delete_if{|package| package.strip.empty?}
    packages.delete_if{|package| File.basename(package) !~ %r/\.(tar\.gz|tgz)$/}
    packages.delete_if{|package| File.basename(package) =~ %r/^ruby.*\.(tar\.gz|tgz)$/}

    Dir.chdir(BUILDDIR){
      unless manifest
        packages = packages.sort_by do |package|
          lines = `tar tfz #{ package }`.split %/\n/
          line = lines.grep(%r/(install\.rb|extconf\.rb|configure)/).first
          dirname, first, rest = line.split(File::SEPARATOR, 2)
          type = first unless rest
          abort "cannot determine type of <#{ package }>" unless type
          case type
            when /configure/
              0
            when /extconf.rb/
              1
            when /install.rb/
              2
          end
        end
      end

      packages.each{|package| install_package package}
    }

    Dir.chdir(".."){
      pwd = File.basename(Dir.pwd)
      log = "#{ pwd }.log" 
      if test ?e, "install.rb"
        puts "installing #{ pwd } (see #{ log } for details)..."
        install_ruby_package log
      end
      if test ?e, "extconf.rb"
        puts "installing #{ pwd } (see #{ log } for details)..."
        install_ruby_extentsion log
      end
      if test ?e, "configure"
        puts "installing #{ pwd } (see #{ log } for details)..."
        install_src_package log
      end
    }
  end
  def install_package package
    case package
      when /(tgz|tar.gz)$/
        install_from_tgz package
      else
        abort "can't install package <#{ package }>"
    end
  end
  def install_from_tgz tgz
    old = filelist '*' 
    spawn "tar xvfz #{ tgz } >/dev/null 2>&1"
    new = filelist '*'
    created = new - old
    expected = File.join(BUILDDIR, File.basename(tgz).gsub(%r/(\.tar\.gz|\.tgz)$/,''))
    dir = created.first || expected 
    log = "#{ BUILDDIR }/#{ File.basename tgz }.log"

    Dir.chdir(dir){
      puts "installing #{ tgz } (see #{ log } for details)..."
      status = nil
      if test ?e, 'install.rb'
        status = install_ruby_package(log)
      end
      if test ?e, 'extconf.rb'
        status = install_ruby_extentsion(log)
      end
      if test ?e, 'configure'
        status = install_src_package(log)
      end
      if status
        puts "success"
      else
        puts "failure"
        exit 1
      end
    }
  end
  def install_ruby_package log
    system "{ #{ SUDO } #{ RUBY } install.rb '--with-cflags=#{ CFLAGS }' '--with-ldflags=#{ LDFLAGS }'; } >#{ log } 2>&1" or
      system "{ #{ SUDO } #{ RUBY } install.rb; } >#{ log } 2>&1"
  end
  def install_ruby_extentsion log
    system "{ #{ RUBY } extconf.rb '--with-cflags=#{ CFLAGS }' '--with-ldflags=#{ LDFLAGS }' && stat [mM]akefile && #{ MAKE } && #{ SUDO } #{ MAKE } install; } >#{ log } 2>&1" or
      system "{ #{ RUBY } extconf.rb && stat [mM]akefile && #{ MAKE } && #{ SUDO } #{ MAKE } install; } >#{ log } 2>&1"
  end
  def install_src_package log
    system "{ #{ MAKE } clean; } >/dev/null 2>&1"
    system "{ ./configure --prefix=#{ PREFIX } && stat [mM]akefile && #{ MAKE } && #{ SUDO } #{ MAKE } install; } >#{ log } 2>&1"
  end
  def spawn cmd
    system cmd or abort "cmd <#{ cmd }> failed"
  end
  def filelist glob
    Dir.glob(glob).map{|path| File.expand_path path}.sort.uniq
  end
end

Installer.new.run
