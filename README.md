#### 创建ODS数据库语句
create database ods_origin location '/data_warehouse/ods_origin.db';

#### 创建metadata数据库语句
create database metadata location '/data_warehouse/metadata.db';

#### 创建ODS原始层表log_origin命令
./odsCreateTable.sh

#### 创建kafak topic命令
./odsCreateTopic.sh log-raw-boikgpokn78sb95kjhfrendo 12
第一个参数为topic名称，第二个参数为partition数量

#### 启动filebeat
./startFilebeat.sh filebeat_log-raw-boikgpokn78sb95kjhfrendo.yml
后面的参数为配置文件名


