#!/bin/sh

#
# workaround so bin/* gets doc'd as ruby files
#

pushd .
cd bin
for f in *; do ln -s $f "$f.rb"; done
popd

rdoc -a -d -F -S -m README -I jpg -N [A-Z]* bin/rq.rb lib/rq.rb lib/*/*

pushd .
cd bin
for f in *rb; do rm -f $f; done
popd
