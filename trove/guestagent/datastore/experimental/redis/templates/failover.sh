#!/bin/bash
# failover.sh <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>

echo Start Failover  >> /var/log/redis/reconfig.log
date >> /var/log/redis/reconfig.log
echo $* >> /var/log/redis/reconfig.log
conf_file=/var/lib/trove/.redis
master_ip=$6
interface=eth0
vip=$( echo $(grep vip $conf_file | awk -F= '{print $2}'))
my_ip=`ip address show $interface | grep -w inet| grep -v $vip | awk '{print $2}' |awk -F/ '{print $1}'`
netmask=32
if [[ $master_ip == $my_ip ]];then
    sudo ip addr add $vip/$netmask dev $interface
    sudo arping -q -c 3 -A $vip -I $interface
    echo Add vip $vip to $my_ip >> /var/log/redis/reconfig.log
else
    sudo ip addr del $vip/$netmask dev $interface
    echo Del vip $vip from $my_ip >> /var/log/redis/reconfig.log
fi

echo End Failover  >> /var/log/redis/reconfig.log
