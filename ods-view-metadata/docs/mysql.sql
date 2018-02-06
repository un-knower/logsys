/*
Navicat MySQL Data Transfer

Source Server         : 测试集群metadata
Source Server Version : 50528
Source Host           : 10.255.129.20:3306
Source Database       : metadata

Target Server Type    : MYSQL
Target Server Version : 50528
File Encoding         : 65001

Date: 2018-02-02 15:14:06
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for applog_key_field_desc
-- ----------------------------
DROP TABLE IF EXISTS `applog_key_field_desc`;
CREATE TABLE `applog_key_field_desc` (
  `appId` varchar(100) NOT NULL COMMENT 'appId',
  `fieldName` varchar(20) NOT NULL COMMENT '字段名称',
  `fieldFlag` int(20) DEFAULT NULL COMMENT '字段标记, 0=库名字段;1=表名字段,2=分区字段',
  `fieldOrder` int(2) DEFAULT NULL COMMENT '字段顺序',
  `fieldDefault` varchar(15) DEFAULT NULL COMMENT '字段默认值',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日志关键字段描述表';

-- ----------------------------
-- Table structure for applog_special_field_desc
-- ----------------------------
DROP TABLE IF EXISTS `applog_special_field_desc`;
CREATE TABLE `applog_special_field_desc` (
  `id` varchar(100) NOT NULL COMMENT '唯一性ID',
  `tabNameReg` varchar(100) NOT NULL COMMENT '表名正则匹配表达式',
  `logPathReg` varchar(100) NOT NULL COMMENT '日志文件路径正则匹配表达式',
  `fieldNameReg` varchar(100) DEFAULT NULL COMMENT '字段名正则匹配表达式',
  `specialType` varchar(100) DEFAULT NULL COMMENT '特例类型, blackList=黑名单,rename=重命名',
  `specialValue` varchar(500) DEFAULT NULL COMMENT '特例值,如重命名值',
  `specialOrder` int(2) DEFAULT NULL COMMENT '排序值',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日志关键字段描述表';

-- ----------------------------
-- Table structure for black_table_info
-- ----------------------------
DROP TABLE IF EXISTS `black_table_info`;
CREATE TABLE `black_table_info` (
  `id` varchar(100) NOT NULL COMMENT '唯一性ID',
  `tableName` varchar(100) DEFAULT NULL COMMENT '表名称',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='元数据管理的黑名单表';

-- ----------------------------
-- Table structure for log_baseinfo
-- ----------------------------
DROP TABLE IF EXISTS `log_baseinfo`;
CREATE TABLE `log_baseinfo` (
  `id` varchar(100) NOT NULL COMMENT '唯一性ID',
  `productCode` varchar(100) NOT NULL COMMENT '产品线code',
  `productCodeId` varchar(100) DEFAULT NULL COMMENT '产品线加密后的id',
  `fieldName` varchar(1000) DEFAULT NULL COMMENT '字段名称',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='baseinfo白名单字段表';

-- ----------------------------
-- Table structure for log_field_type_info
-- ----------------------------
DROP TABLE IF EXISTS `log_field_type_info`;
CREATE TABLE `log_field_type_info` (
  `id` varchar(100) NOT NULL COMMENT '唯一性ID',
  `name` varchar(100) DEFAULT NULL COMMENT 'ruleLevel 为table则为表名，为realLogType 则为realLogType，为field 则为 ALL',
  `fieldName` varchar(20) DEFAULT NULL COMMENT '字段名称',
  `fieldType` varchar(20) DEFAULT NULL COMMENT '字段类型',
  `typeFlag` varchar(2) DEFAULT NULL COMMENT '字段类型标识 1:String 2:Long 3:Double 4.array 5.arrayString 6.arrayLong 7.arrayStruct',
  `ruleLevel` varchar(15) DEFAULT NULL COMMENT '规则类型 1.table:表级别 2.realLogType:日志类型级别 3.field:字段级别',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日志文件中字段的类型';

-- ----------------------------
-- Table structure for logfile_field_desc
-- ----------------------------
DROP TABLE IF EXISTS `logfile_field_desc`;
CREATE TABLE `logfile_field_desc` (
  `logPath` varchar(200) NOT NULL COMMENT '日志文件路径',
  `fieldName` varchar(50) NOT NULL COMMENT '字段名',
  `fieldType` varchar(100) DEFAULT NULL COMMENT 'hive字段类型',
  `fieldSql` varchar(1000) DEFAULT NULL COMMENT 'hive字段SQL声明子句',
  `rawType` varchar(100) DEFAULT NULL COMMENT 'parquet字段类型',
  `rawInfo` varchar(1000) DEFAULT NULL COMMENT 'parquet字段类型信息',
  `taskId` varchar(100) DEFAULT NULL COMMENT '产生该批数据的任务ID',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日志文件字段信息表';

-- ----------------------------
-- Table structure for logfile_key_field_value
-- ----------------------------
DROP TABLE IF EXISTS `logfile_key_field_value`;
CREATE TABLE `logfile_key_field_value` (
  `logPath` varchar(200) NOT NULL COMMENT '日志文件路径',
  `fieldName` varchar(50) NOT NULL COMMENT '字段名',
  `fieldValue` varchar(200) DEFAULT NULL COMMENT '字段值',
  `appId` varchar(200) DEFAULT NULL COMMENT 'appId',
  `taskId` varchar(100) DEFAULT NULL COMMENT '产生该批数据的任务ID',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for logtab_ddl
-- ----------------------------
DROP TABLE IF EXISTS `logtab_ddl`;
CREATE TABLE `logtab_ddl` (
  `seq` int(255) NOT NULL AUTO_INCREMENT,
  `dbName` varchar(50) NOT NULL,
  `tabName` varchar(100) NOT NULL,
  `taskId` varchar(100) DEFAULT NULL COMMENT '产生该批数据的任务ID',
  `ddlType` varchar(255) DEFAULT NULL COMMENT 'DML类型 ADD_PARTITION',
  `ddlText` varchar(3000) DEFAULT NULL COMMENT 'DML语句',
  `commitTime` timestamp NULL DEFAULT NULL COMMENT 'DDL提交时间',
  `commitCode` int(10) DEFAULT NULL COMMENT 'DDL提交结果',
  `commitMsg` varchar(255) DEFAULT NULL COMMENT 'DDL提交结果说明',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间',
  PRIMARY KEY (`seq`)
) ENGINE=InnoDB AUTO_INCREMENT=534 DEFAULT CHARSET=utf8 COMMENT='DDL信息表';

-- ----------------------------
-- Table structure for logtab_dml
-- ----------------------------
DROP TABLE IF EXISTS `logtab_dml`;
CREATE TABLE `logtab_dml` (
  `seq` int(255) NOT NULL AUTO_INCREMENT,
  `dbName` varchar(50) NOT NULL,
  `tabName` varchar(100) NOT NULL,
  `taskId` varchar(100) DEFAULT NULL COMMENT '产生该批数据的任务ID',
  `dmlType` varchar(255) DEFAULT NULL COMMENT 'DML类型 ADD_PARTITION',
  `dmlText` varchar(3000) DEFAULT NULL COMMENT 'DML语句',
  `commitTime` timestamp NULL DEFAULT NULL COMMENT 'DDL提交时间',
  `commitCode` int(10) DEFAULT NULL COMMENT 'DDL提交结果',
  `commitMsg` varchar(255) DEFAULT NULL COMMENT 'DDL提交结果说明',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间',
  PRIMARY KEY (`seq`)
) ENGINE=InnoDB AUTO_INCREMENT=1539 DEFAULT CHARSET=utf8 COMMENT='DML信息表';

-- ----------------------------
-- Table structure for logtab_field_desc
-- ----------------------------
DROP TABLE IF EXISTS `logtab_field_desc`;
CREATE TABLE `logtab_field_desc` (
  `seq` int(255) NOT NULL AUTO_INCREMENT,
  `dbName` varchar(50) NOT NULL,
  `tabName` varchar(100) NOT NULL,
  `fieldName` varchar(200) NOT NULL COMMENT '字段名',
  `taskId` varchar(100) DEFAULT NULL COMMENT '产生该批数据的任务ID',
  `fieldType` varchar(255) DEFAULT NULL COMMENT 'hive字段类型',
  `fieldSql` varchar(255) DEFAULT NULL COMMENT 'hive字段SQL声明子句',
  `isDeleted` tinyint(1) DEFAULT NULL COMMENT '记录是否删除',
  `createTime` timestamp NULL DEFAULT NULL COMMENT '记录创建时间',
  `updateTime` timestamp NULL DEFAULT NULL COMMENT '记录更新时间',
  PRIMARY KEY (`seq`)
) ENGINE=InnoDB AUTO_INCREMENT=43233 DEFAULT CHARSET=utf8 COMMENT='表字段信息表';
