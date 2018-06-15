#!/bin/sh

# call as:  ./mk_gw K309AK0004
# GW=$1


PORT=`cat port`
#IMG="debIoT20180319.img"

IMG="debIoT20180502"

SRCIMG="${IMG}.img"
DESTIMG="${IMG}.img"

for gw in *.pk
do
    if [ "${gw}" = "*.pk" ] ; then
      echo No pk files
      exit 1
    fi
    GW=`basename $gw .pk`

    PRJ=`/data2/efs/spool/nathan/project/get_hubs.sh -g ${GW} | tr -d '"' `
    if [ "${PRJ}" = "" ] ; then
      echo $GW not in hubs.json
      exit 1
    fi

    case ${PRJ} in
      NEWhubs)
        echo NewHubs
        ;;
      Amsterdam)
        echo Amsterdam
        SRCIMG="debIoT20180502-amst.img"
        DESTIMG="debIoT20180502"
        ;;
      Stadskanaal)
        echo Stadskanaal
        SRCIMG="debIoT20180502-stads.img"
        DESTIMG="debIoT20180502"
        ;;
      Groningen)
        echo Groningen
        SRCIMG="debIoT20180502-gron.img"
        DESTIMG="debIoT20180502"
        ;;
      Heerhugowaard)
        echo Heerhugowaard
        SRCIMG="debIoT20180528.img"
        DESTIMG=${SRCIMG}
        ;;
      Oss)
        echo Oss
        ;;
      Protos)
        echo Protos
        ;;
      *)
        echo Unknown project
        exit 1
        ;;
    esac

    mkdir -p $GW/log
    mkdir -p $GW/data
    if [ ! -s ${GW}.pk ] ; then
      echo wrong file: ${GW}.pk missing
      exit 1
    fi
    MAC=`cat ${GW}.pk  | cut -d' ' -f2`
    echo $PORT >${GW}/port
    echo $PORT $MAC >${GW}/port.vpn
    NW_PORT=`expr $PORT + 1`
    echo $NW_PORT >port
    cp .images/${SRCIMG} ${GW}/${DESTIMG}
    echo update img with  ${SRCIMG}
    grep -q ${GW} project.stat
    if [ $? -eq 1 ]
    then
        echo ${GW} ${MAC} pk >>project.stat
    else
        # entry already exists!
        # TODO:  overwrite/chamge to pk img entry
        echo replace ${GW} entry in project .stat with...
        echo ${GW} ${MAC} pk img
    fi

    rm -f ${GW}/newimg ${GW}/update ${GW}/updated

    echo ${GW}'/s/$/ img/' project.stat >project.stat1
    sed '/'${GW}'/s/$/ img/' project.stat >project.stat1
    mv project.stat1 project.stat
    
    KEY=`cat ${GW}.pk  | cut -d' ' -f1`
    echo ssh-rsa ${KEY} root@${GW} >>../.ssh/authorized_keys
    mv ${GW}.pk ${GW}
    scp ${GW}/${GW}.pk  nathan@172.31.16.135:~/GW/
    scp ${GW}/port.vpn  nathan@172.31.16.135:~/GW/${GW}.port
    ssh nathan@172.31.16.135 '~/add_key.sh'
    rm -f ${GW}/port.vpn
done
