
cd `dirname $0`
cd ../datas/
pwd=`pwd`

filepath=$pwd/logcenter-tv.log
echo $filepath

while read -r LINE
do
    echo $LINE > /tmp/req.tmp
    ab -c 1 -n 1 -H "Content-Type:application/json" -p /tmp/req.tmp http://logupload.aginomoto.com:8180/
done < $filepath


http://log.moretv.com.cn:8180/activity
http://log.moretv.com.cn:8180/medusalog
http://log.moretv.com.cn:8180/moretv/userdurationlog

http://log.moretv.com.cn:8180/uploadlog
http://log.moretv.com.cn:8180/uploadplaylog

http://log.moretv.com.cn:8180/mtvkidslog
http://log.moretv.com.cn:8180/mobilehelperlog
http://log.moretv.com.cn:8180/mobilelog

http://vrlog.aginomoto.com:8180/vrapplog
http://crawlerlog.aginomoto.com:8180/price






