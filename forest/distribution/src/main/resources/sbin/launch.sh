#!/bin/bash

#################################################################
# example:
#   ./launch.sh --task=forest_actionlog_start
#   ./launch.sh --task=forest_actionlog_stop
#################################################################

cd `dirname $0`
source /etc/profile
source ~/.bash_profile
source ../bin/envFn.sh

load_args $*

case "$task" in
    #--f 逗号分隔的配置文件清单
    # 每个配置文件支持的形式为： resource://{resourcePath};  file://{filePath}; /{filePath}; {resourcePath}
    forest_actionlog_start)
        export taskName=forest_actionlog
        ../bin/forest_actionlog.sh start --f MsgBatchManager.xml
    ;;
    forest_actionlog_stop)
        export taskName=forest_actionlog
        ../bin/forest_actionlog.sh stop
    ;;
    *)
        echo invalid task $task
    ;;
esac
