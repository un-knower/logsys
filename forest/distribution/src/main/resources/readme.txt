###kill all######
ps -ef|grep MsgProc|awk '{print $2}'|grep -v grep|xargs kill
ps -ef|grep MsgProc

--------------------------------------------------------------------------------------------
### boikgpokn78sb95ktmsc1bnk	whaley	medusa
./sbin/launch_msgproc.sh start --taskName=medusa 
tail -f /data/logs/forest/msgproc_medusa.log
./sbin/launch_msgproc.sh stop --taskName=medusa

--------------------------------------------------------------------------------------------
### boikgpokn78sb95kjhfrendo	whaley	whaleytv
./sbin/launch_msgproc.sh start --taskName=whaleytv
tail -f /data/logs/forest/msgproc_whaleytv.log
./sbin/launch_msgproc.sh stop  --taskName=whaleytv

----------------------------------------------
./sbin/launch_msgproc.sh start --groupId=forest-dist-whaleytv --taskName=whaleytv --taskId=2
./sbin/launch_msgproc.sh start --groupId=forest-dist-whaleytv --taskName=whaleytv --taskId=3
tail -f /data/logs/forest/msgproc_whaleytv2.log
tail -f /data/logs/forest/msgproc_whaleytv3.log
./sbin/launch_msgproc.sh stop  --taskName=whaleytv
./sbin/launch_msgproc.sh stop  --taskName=whaleytv2
./sbin/launch_msgproc.sh stop  --taskName=whaleytv3
----------------------------------------------


--------------------------------------------------------------------------------------------
### boikgpokn78sb95kbqei6cc9	whaley	whaleyvr
### boikgpokn78sb95kicggqhbk	whaley	orca
./sbin/launch_msgproc.sh start --taskName=whaleyvr_orca 
tail -f /data/logs/forest/msgproc_whaleyvr_orca.log
./sbin/launch_msgproc.sh stop  --taskName=whaleyvr_orca

--------------------------------------------------------------------------------------------
### boikgpokn78sb95k7id7n8eb	whaley	eagle
### boikgpokn78sb95kjtihcg26	whaley	mobilehelper
./sbin/launch_msgproc.sh start --taskName=eagle_mobilehelper 
tail -f /data/logs/forest/msgproc_eagle_mobilehelper.log
./sbin/launch_msgproc.sh stop --taskName=eagle_mobilehelper

--------------------------------------------------------------------------------------------
### boikgpokn78sb95kkls3bhmt	whaley	crawler
./sbin/launch_msgproc.sh start --taskName=crawler 
tail -f /data/logs/forest/msgproc_crawler.log
./sbin/launch_msgproc.sh stop --taskName=crawler


