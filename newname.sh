#!/bin/sh

# script to rename all of blackhole to something else
# the name should be lower case and not end in _fdw (the script adds _fdw
# where needed)

newname=$1

if test -z "$newname"
then
   echo usage: $0 newname
   exit 1;
fi

if expr "$newname" : ".*_fdw" > /dev/null
then
   echo "remove _fdw from new name - script will use it where appropriate"
   exit 1
fi


Newname=`perl -e "print qq(\\u$newname);"`


grep --exclude-dir=.git --exclude=newname.sh -rl blackhole . | xargs sed -i -e "s/blackhole/$newname/g"
grep --exclude-dir=.git --exclude=newname.sh -rl Blackhole . | xargs sed -i -e "s/Blackhole/$Newname/g"

mv blackhole_fdw.control ${newname}_fdw.control
mv src/blackhole_fdw.c src/${newname}_fdw.c
mv doc/blackhole_fdw.md doc/${newname}_fdw.md
mv sql/blackhole_fdw.sql sql/${newname}_fdw.sql
mv test/sql/blackhole.sql test/sql/${newname}.sql
mv test/expected/blackhole.out test/expected/${newname}.out





