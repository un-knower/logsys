

for host in bigdata-computing-02-013 bigdata-computing-02-014 bigdata-computing-02-015 bigdata-computing-02-016 bigdata-computing-02-017 bigdata-computing-02-021
do
	ssh $host "grep -i access_log  /opt/openresty/nginx/conf/nginx.conf" > /tmp/$host.tmp
	for file in `cat /tmp/$host.tmp|grep -v "#"|grep -v off|awk '{print $2}'`
	do
		#echo "ssh $host \" echo $host;ls -l $file \"  "
		echo $host
		ssh $host  ls -lh $file
	done
done


for host in bigdata-computing-02-013 bigdata-computing-02-014 bigdata-computing-02-015 bigdata-computing-02-016 bigdata-computing-02-017 bigdata-computing-02-021
do
	ssh $host "grep -i access_log  /opt/openresty/nginx/conf/online/*.conf" > /tmp/$host.tmp
	for file in `cat /tmp/$host.tmp|grep -v "#"|grep -v off|awk '{print $3}'`
	do
		#echo "ssh $host \" echo $host;ls -l $file \"  "
		echo $host
		ssh $host  ls -lh $file
	done
done


for host in bigdata-computing-02-013 bigdata-computing-02-014 bigdata-computing-02-015 bigdata-computing-02-016 bigdata-computing-02-017 bigdata-computing-02-021
do
	ssh $host "grep -i access_log  /opt/openresty/nginx/conf/online/*.conf" > /tmp/$host.tmp
	cat /tmp/$host.tmp|grep -v "#"|grep -v off|awk '{print $3}'
done



ssh bigdata-computing-02-013 " echo bigdata-computing-02-013;ls -l /var/log/nginx/access.log "
ssh bigdata-computing-02-014 " echo bigdata-computing-02-014;ls -l /var/log/nginx/access.log "
ssh bigdata-computing-02-015 " echo bigdata-computing-02-015;ls -l /var/log/nginx/access.log "
ssh bigdata-computing-02-016 " echo bigdata-computing-02-016;ls -l /var/log/nginx/userdurationlog.access.log "
ssh bigdata-computing-02-016 " echo bigdata-computing-02-016;ls -l /var/log/nginx/priceinfo.access.log "
ssh bigdata-computing-02-017 " echo bigdata-computing-02-017;ls -l /var/log/nginx/access.log "
ssh bigdata-computing-02-021 " echo bigdata-computing-02-021;ls -l /var/log/nginx/access.log "

