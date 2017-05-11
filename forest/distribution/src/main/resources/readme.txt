###kill all######
ps -ef|grep MsgProc|awk '{print $2}'|grep -v grep|xargs kill
ps -ef|grep MsgProc

### boikgpokn78sb95ktmsc1bnk	whaley	medusa
topicRegex='^log-raw-boikgpokn78sb95ktmsc1bnk.*$'
./sbin/launch_msgproc.sh start --taskName=medusa --topicRegex=$topicRegex
tail -f /data/logs/forest/msgproc_medusa.log
./sbin/launch_msgproc.sh stop --taskName=medusa

### boikgpokn78sb95kjhfrendo	whaley	whaleytv
topicRegex='^log-raw-boikgpokn78sb95kjhfrendo.*$'
./sbin/launch_msgproc.sh start --taskName=whaleytv --topicRegex=$topicRegex
tail -f /data/logs/forest/msgproc_whaleytv.log
./sbin/launch_msgproc.sh stop  --taskName=whaleytv

### boikgpokn78sb95kbqei6cc9	whaley	whaleyvr
### boikgpokn78sb95kicggqhbk	whaley	orca
topicRegex='(^log-raw-boikgpokn78sb95kbqei6cc9.*$)|(^log-raw-boikgpokn78sb95kicggqhbk.*$)'
./sbin/launch_msgproc.sh start --taskName=whaleyvr_orca --topicRegex=$topicRegex
tail -f /data/logs/forest/msgproc_whaleyvr_orca.log
./sbin/launch_msgproc.sh stop  --taskName=whaleyvr_orca

### boikgpokn78sb95k7id7n8eb	whaley	eagle
### boikgpokn78sb95kjtihcg26	whaley	mobilehelper
topicRegex='(^log-raw-boikgpokn78sb95k7id7n8eb.*$)|(^log-raw-boikgpokn78sb95kjtihcg26.*$)'
./sbin/launch_msgproc.sh start --taskName=eagle_mobilehelper --topicRegex=$topicRegex
tail -f /data/logs/forest/msgproc_eagle_mobilehelper.log
./sbin/launch_msgproc.sh stop --taskName=eagle_mobilehelper

### boikgpokn78sb95kkls3bhmt	whaley	crawler
topicRegex='^log-raw-boikgpokn78sb95kkls3bhmt.*$'
./sbin/launch_msgproc.sh start --taskName=crawler --topicRegex=$topicRegex
tail -f /data/logs/forest/msgproc_crawler.log
./sbin/launch_msgproc.sh stop --taskName=crawler


