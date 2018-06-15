GW=$1

MAC=`cat .tva/${GW}.pk  | cut -d' ' -f2`
PORT=`awk '{ print $1}' ${GW}/port`
cp .tva/${GW}.pk ${GW}
echo "$PORT $MAC"  >${GW}/port
scp ${GW}/port  nathan@172.31.16.135:~/GW/${GW}.port
