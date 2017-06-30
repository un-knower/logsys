#!/usr/bin/env bash

nohup sh submit.sh cn.whaley.bi.logsys.log2parquet.MainObj MsgProcExecutor --c inputPath=/data_warehouse/ods_origin.db/log_origin/key_appId=boikgpokn78sb95ktmsc1bnkechpgj9l/key_day=20170614/key_hour=13/boikgpokn78sb95ktmsc1bnkechpgj9l_2017061413_raw_7_575892351.json.gz > test.log 2>&1 &