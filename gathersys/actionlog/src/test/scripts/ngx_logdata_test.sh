
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



