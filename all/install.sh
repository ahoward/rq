#!/bin/bash


###############################################################################
#
# CONFIG SECTION - EDIT TO SUIT YOUR SITE 
#
###############################################################################
#
# prefix : everything will be installed under this directory.  it should be an
# absolute pathname.
#
  export prefix="$1"
#
# libdir : libs will installed here
#
  export libdir="${prefix}/lib"
#
# incdir : libs will installed here
#
  export incdir="${prefix}/include"
#
# bindir : executables and scripts will installed here
#
  export bindir="${prefix}/bin"
#
# sudo : set to "sudo" if you need this to install into prefix
#
  export sudo="$SUDO"  # note that you can affect this var via 'export SUDO=sudo'
#
# full path of dir containing packages to be installed
#
  export packagedir="`pwd`/packages"
#
# full path of dir where packages will be built 
#
  export builddir="`pwd`/build"
#
# set this to true if you want to install ruby into prefix too.  otherwise it
# automatically determines if ruby needs to be installed.
#
  install_ruby=`test -f ${bindir}/ruby && echo false || echo true`
  echo "install_ruby: $install_ruby"
  #install_ruby='false'

  install_gem=`test -f ${bindir}/gem && echo false || echo true`
  echo "install_gem: $install_gem"
  #install_gem='false'
#
# determine ruby tar ball - this just picks up latest version
#
  ruby_tgz=`ls ${packagedir}/ruby-*gz|sort|tail -1`


###############################################################################
#
# INSTALL SECTION - DO **NOT** EDIT
#
###############################################################################
#
# vars 
#
  div="==============================================================================="
  line="-------------------------------------------------------------------------------"
  usage="usage : $0 prefix"
#
# check/create prefix
#
  if [[ ! -n "$prefix" ]]; then
    printf "$usage\n"
    exit 1
  fi
  if [[ ! -d "$prefix" ]]; then
    $sudo mkdir -p $prefix 
  fi
  if [[ ! -d "$prefix" ]]; then
    "prefix <$prefix> does not exist and could not be created"
    exit 1
  fi
#
# check/create incdir/libdir/bindir
#
  if [[ ! -d "$incdir" ]]; then
    $sudo mkdir -p $incdir 
  fi
  if [[ ! -d "$incdir" ]]; then
    "incdir <$incdir> does not exist and could not be created"
    exit 1
  fi
  if [[ ! -d "$libdir" ]]; then
    $sudo mkdir -p $libdir 
  fi
  if [[ ! -d "$libdir" ]]; then
    "libdir <$libdir> does not exist and could not be created"
    exit 1
  fi
  if [[ ! -d "$bindir" ]]; then
    $sudo mkdir -p $bindir 
  fi
  if [[ ! -d "$bindir" ]]; then
    "bindir <$bindir> does not exist and could not be created"
    exit 1
  fi
#
# info 
#
  printf "\n$div\n"
  printf "CONFIG\n"
  printf -- "$line\n"
  printf "prefix <$prefix>\n"
  printf "libdir <$libdir>\n"
  printf "incdir <$incdir>\n"
  printf "bindir <$bindir>\n"
  printf "packagedir <$packagedir>\n"
  printf "builddir <$builddir>\n"
  printf -- "$line\n"
#
# important env settings for proper compilation
#
  export LD_RUN_PATH="${libdir}"
  export LD_LIBRARY_PATH="${libdir}"
  export LDFLAGS="-L${libdir}"
  export CFLAGS="-I${incdir}"
  export PATH="${bindir}:$PATH"
#
# important aliases for proper complilation and installation
#
  export make="env LD_RUN_PATH=${libdir} LD_LIBRARY_PATH=${libdir} make"
  export MAKE="env LD_RUN_PATH=${libdir} LD_LIBRARY_PATH=${libdir} make"
  export ruby=ruby #"${bindir}/ruby"
#
# pre-install
#
  cwd=`pwd`
  pushd . >/dev/null
  mkdir -p "$builddir"
  if [[ $? != 0 ]];then
    printf "COULD NOT CREATE BUILDIR <${builddir}>" 1>&2
    popd >/dev/null
    exit 1
  fi
#
# install ruby iff needed 
#
  if [[ "$install_ruby" == *true* ]]; then
    cd "$builddir"
    log="${builddir}/`basename $ruby_tgz`.log"
    printf "\n$div\n"
    printf "installing $ruby_tgz (see $log for details)...\n"

    (tar xvfz "$ruby_tgz" &&\
     cd ruby* &&\
    ./configure "--prefix=${prefix}" &&\
    $make &&\
    $sudo $make install) > "$log" 2>&1

    if [[ $? == 0 ]];then
      printf "success\n"
    else
      printf "failure\n"
      cd "$cwd"
      exit 1
    fi
    printf -- "$line\n"
    cd "$cwd"
  fi
#
# use ruby to bootstrap everything else via ./install.rb
#
  $ruby ./install.rb || exit 1
#
# serve notice
#
  printf -- "\n$div\n"
  printf "ATTENTION\n"

  printf -- "$line\n"
  printf "IT APPEARS THAT INSTALLATION WAS COMPLETE AND SUCCESSFUL\n"
  printf "\n"
  printf "THE FOLLOWING ENVIRONMENT SETTINGS ARE RECCOMENDED WHEN RUNNING THE SOFTWARE\n"
  printf "  bash/sh:\n"
  printf "    export PATH=\"${bindir}:\$PATH\"\n"
  printf "    export LD_LIBRARY_PATH=\"${libdir}\"\n"
  printf "  tcsh/csh:\n"
  printf "    setenv PATH \"${bindir}:\$PATH\"\n"
  printf "    setenv LD_LIBRARY_PATH \"${libdir}\"\n"
  printf "\n"
  printf "YOU CAN CREATE A QUEUE USING\n"
  printf "  ${bindir}/rq ./q create\n"
  printf "\n"
  printf "YOU CAN SUBMIT A JOB TO THAT QUEUE USING\n"
  printf "  ${bindir}/rq ./q submit echo 42\n"
  printf "\n"
  printf "YOU CAN RUN JOBS FROM THAT QUEUE IN THE FOREGROUND USING\n"
  printf "  ${bindir}/rq ./q feed\n"
  printf "\n"
  printf "YOU CAN RUN JOBS FROM THAT QUEUE IN THE BACKGROUND AND FOREVER USING\n"
  printf "  ${bindir}/rq ./q cronify\n"
  printf "\n"
  printf "FOR MORE INFORMATION TYPE\n"
  printf "  ${bindir}/rq help\n"
  printf "\n"
  printf "\n"
#
# post-install 
#
  popd >/dev/null
  exit 0
