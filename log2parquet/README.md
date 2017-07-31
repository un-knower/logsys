####项目介绍
* 项目作用
1.将ods按小时分割的数据，做数据清洗并转化为parquet文件
2.生成供`元数据模块`使用的基础信息


####azkaban部署
项目名称：ods_view_log2parquet

####测试运行环境【物理机器提交】
机器：spark@bigdata-appsvr-130-5
测试运行目录： /app/ods_view_log2parquet




boikgpokn78sb95ktmsc1bnken8tuboa    medusa2.x
boikgpokn78sb95ktmsc1bnkechpgj9l    medusa3.x
boikgpokn78sb95kjhfrendo8dc5mlsr    whaley

boikgpokn78sb95kjhfrendoj8ilnoi7   【whaleytv，  wui2.0】 
boikgpokn78sb95kjhfrendoepkseljn   【whaleytv，global_menu_2 ，全局菜单2.0】 
boikgpokn78sb95kjhfrendobgjgjolq   【whaleytvepop ，线下店演示用的应用，作用是 保证所有电视播的画面是同步的】


[hadoop@bigdata-appsvr-130-5 ~]$ hadoop fs -ls /data_warehouse/ods_origin.db/log_origin/
Found 15 items
drwxr-xr-x   - hadoop hadoop          0 2017-07-16 01:05 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95k0000000000000000
drwxr-xr-x   - hadoop hadoop          0 2017-07-16 00:47 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95k7id7n8eb8dc5mlsr
drwxr-xr-x   - hadoop hadoop          0 2017-07-16 00:10 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kbqei6cc98dc5mlsr
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 00:07 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendo8dc5mlsr
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 04:30 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendobgjgjolq
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 00:05 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendoepkseljn
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 01:10 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendoj8ilnoi7
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 01:10 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendojtihcg26
drwxr-xr-x   - hadoop hadoop          0 2017-07-21 11:25 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendoqgj2fgqh
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 00:49 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95kjhfrendosesh6bmu
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 01:12 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkbe9pbhgu
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 00:48 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 01:12 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnken8tuboa
drwxr-xr-x   - hadoop hadoop          0 2017-07-31 01:12 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkfipphckl
drwxr-xr-x   - hadoop hadoop          0 2017-07-16 00:28 /data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkklf477ap