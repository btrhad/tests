#!/usr/local/bin/bash

GW=$1

rm -f ${GW}gw_dict.*
DATE="0"
for i in $GW/log/*
do
    if [ -f $i ] ; then
        echo $i
        POSIX=`basename $i .log | sed "s/^${GW}//"`
        tar xOzf $i home/tls/confs.tgz | tar xOzf - www/gw_dict.json >${GW}/gw.tmp
        if [ $DATE = "0" ] ; then
            mv ${GW}/gw.tmp ${GW}/gw_dict.${POSIX}
            DATE=${POSIX}
        else
            diff ${GW}/gw_dict.${DATE} ${GW}/gw.tmp >/dev/null
            RES=$?
            if [ $RES -eq 1 ] ; then
                mv ${GW}/gw.tmp ${GW}/gw_dict.${POSIX}
                DATE=${POSIX}
            fi
        fi
        rm -f ${GW}/gw.tmp
    fi
done
