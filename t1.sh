# first add mac address to ${GW}/port
# and Call as ./t1.sh NNM06A  (=$GW)
# this will add MAC to $GW.pk and botyh port files in this computer and on ec2-demoBSD11
#
# still to be done for: NOMXY (obsolete) , NNM02B (if MAC adresses are known)

GW=$1

KEY=`awk '{ print $1}' ${GW}/${GW}.pk`
PORT=`cat ${GW}/port  | cut -d' ' -f1`
MAC=`cat ${GW}/port  | cut -d' ' -f2`

echo "$KEY $MAC"  >${GW}/${GW}.pk
echo "$PORT $MAC"  >${GW}/port
scp ${GW}/port  nathan@172.31.16.135:~/GW/${GW}.port

