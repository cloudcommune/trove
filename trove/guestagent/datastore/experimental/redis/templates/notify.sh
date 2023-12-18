#!/bin/bash

vm_status_mon="/usr/lib/python2.7/site-packages/trove/guestagent/datastore/experimental/redis/templates/vm_status_mon.py"
date >> /var/log/redis/notify.log
echo $* >> /var/log/redis/notify.log
#+odown master mymaster 192.168.0.11 6379 #quorum 1/1

if [[ $1 == +odown ]];then
    name=`echo $2|awk '{print $2}'`
    quorum=`echo $2|awk '{print $6}'`
    ip=`echo $2|awk '{print $3}'`
    if [[ $quorum == '1/1' ]]; then
        if grep requirepass /etc/redis.conf; then
            passwd=`grep requirepass /etc/redis.conf | awk '{print $2}'`
            passwdopt="-a $passwd"
        else
            passwdopt=""
        fi
        echo Start Failover $name >> /var/log/redis/notify.log
        for i in `seq 20`; do
            echo try failover $name $i >>  /var/log/redis/notify.log
            if sudo redis-cli -p 26379 $passwdopt sentinel failover $name | grep INPROG; then
                sleep 1
            else
                echo Success Failover $name >> /var/log/redis/notify.log
                break
            fi
        done
        echo End Failover $name >> /var/log/redis/notify.log
    fi
    # send notify when node odown
    if [ -f $vm_status_mon ];then
        if [ ! -x $vm_status_mon ];then
            sudo chmod +x $vm_status_mon
        fi
        sudo $vm_status_mon odown $ip
    else
        echo File not exist $vm_status_mon >> /var/log/redis/notify.log
    fi
fi
