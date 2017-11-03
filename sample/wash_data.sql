/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50172
Source Host           : localhost:3306
Source Database       : wash_data

Target Server Type    : MYSQL
Target Server Version : 50172
File Encoding         : 65001

Date: 2017-11-03 20:00:25
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for err_task
-- ----------------------------
DROP TABLE IF EXISTS `err_task`;
CREATE TABLE `err_task` (
  `_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `task_id` int(10) DEFAULT NULL,
  `server` varchar(64) DEFAULT NULL,
  `wash_time` datetime DEFAULT NULL,
  `err_msg` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`_id`)
) ENGINE=InnoDB AUTO_INCREMENT=167 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for tt_exec
-- ----------------------------
DROP TABLE IF EXISTS `tt_exec`;
CREATE TABLE `tt_exec` (
  `_id` int(20) NOT NULL AUTO_INCREMENT,
  `task_id` int(10) DEFAULT NULL,
  `begin_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  PRIMARY KEY (`_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
