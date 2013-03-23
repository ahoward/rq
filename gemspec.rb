lib = 'rq'

dirname = File.dirname(__FILE__)
lines = IO.readlines(File.join('lib/rq.rb'))
line = lines.detect{|line| line =~ /VERSION =/}
version = line.split(/=/).last.scan(/[\d.]+/).first

require 'rubygems'

Gem::Specification::new do |spec|
  $VERBOSE = nil

  shiteless = lambda do |list|
    list.delete_if do |file|
      file =~ %r/\.svn/ or
      file =~ %r/\.tmp/
    end
  end

  spec.name = lib 
  spec.description = 'ruby queue is a zero-admin zero-configuration tool used to create instant unix clusters'
  spec.name = lib 
  spec.version = version 
  spec.platform = Gem::Platform::RUBY
  spec.summary = lib 

  spec.files = shiteless[Dir::glob("**/**")]
  spec.executables = shiteless[Dir::glob("bin/*")].map{|exe| File::basename exe}
  
  spec.require_path = "lib" 

  spec.has_rdoc = File::exist? "doc" 
  spec.test_suite_file = "test/#{ lib }.rb" if File::directory? "test"

  spec.extensions << "extconf.rb" if File::exists? "extconf.rb"

  spec.rubyforge_project = 'codeforpeople'
  spec.author = "Ara T. Howard"
  spec.email = "ara.t.howard@gmail.com"
  spec.homepage = "http://codeforpeople.com/lib/ruby/#{ lib }/"

  %w( arrayfields lockfile posixlock ).each do |depend|
    spec.add_dependency depend
  end
end
