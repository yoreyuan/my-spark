/*
电商广告点击系统数据库表设计

广告点击                  |   timestamp、ip、userID、adID、province、city、clickedCount
广告点击趋势              |    date、hour、minute、adID、clickedCount
广告点击每省份排名前5数据   |   timestamp、adID、province、clickedCount
黑名单数据                |   name
广告点击计数              |   timestamp、adID、province、city、clickedCount

 */

-- use sparkstreaming;
use AdClicked;

-- 建立adclicked广告点击表
CREATE TABLE IF NOT EXISTS adclicked(
  timestamp VARCHAR(255) COMMENT '时间戳',
  ip VARCHAR(255) COMMENT 'IP地址',
  userID VARCHAR(255) COMMENT '用户ID',
  adID VARCHAR(255) COMMENT '广告ID',
  province VARCHAR(255) COMMENT '省份',
  city VARCHAR(255) COMMENT '城市',
  clickedCount INT(10) DEFAULT '0' COMMENT '点击次数'
);


-- 建立adclickedtrend广告点击趋势表
CREATE TABLE IF NOT EXISTS adclickedtrend(
  date VARCHAR(255) COMMENT '日期',
  hour VARCHAR(255) COMMENT '小时',
  minute VARCHAR(255) COMMENT '分钟',
  adID VARCHAR(255) COMMENT '广告ID',
  clickedCount INT(10) DEFAULT '0' COMMENT '点击次数'
);


-- 建立adprovincetopn各省广告点击TopN表
CREATE TABLE IF NOT EXISTS adprovincetopn(
  timestamp VARCHAR(255) COMMENT '时间戳',
  adID VARCHAR(255) COMMENT '广告ID',
  province VARCHAR(255) COMMENT '省份',
  clickedCount INT(10) DEFAULT '0' COMMENT '点击次数'
);


-- 建立blacklisttable黑名单表
CREATE TABLE IF NOT EXISTS blacklisttable(
  name VARCHAR(255) COMMENT '姓名'
);


-- 建立adclickedcount广告点击统计表
CREATE TABLE IF NOT EXISTS adclickedcount(
  timestamp VARCHAR(255) COMMENT '时间戳',
  adID VARCHAR(255) COMMENT '广告ID',
  province VARCHAR(255) COMMENT '身份',
  city VARCHAR(255) COMMENT '城市',
  clickedCount INT(10) DEFAULT '0' COMMENT '点击次数'
);



/*
  查看创建的表的个字段属性
 */
 show tables ;

 desc adclicked;
 desc adclickedtrend;
 desc adprovincetopn;
 desc blacklisttable;
 desc adclickedcount;

