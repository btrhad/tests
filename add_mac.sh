#!/bin/sh
# reconstruct project.stat from project.stat with only hub numbers...
# sh add_mac.sh >project.stat2
# rm project.stat

for i in `cat project.stat`
do
    MAC=`cat $i/$i.pk | awk '{ print $2}'`
    if [ -s $i/updated ] ; then
        echo "$i $MAC pk img newimg update updated"
    else
        if [ -s $i/newimg ] ; then
            echo "$i $MAC pk img newimg"
        else
            echo "$i $MAC pk img"
        fi    
    fi    
done
