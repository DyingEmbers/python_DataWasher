/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50172
Source Host           : localhost:3306
Source Database       : washer_cfg

Target Server Type    : MYSQL
Target Server Version : 50172
File Encoding         : 65001

Date: 2017-11-03 20:00:46
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for server_list
-- ----------------------------
DROP TABLE IF EXISTS `server_list`;
CREATE TABLE `server_list` (
  `id` int(20) NOT NULL,
  `game` varchar(255) DEFAULT NULL,
  `server_id` varchar(255) DEFAULT NULL,
  `ip` varchar(255) DEFAULT NULL,
  `port` int(11) DEFAULT NULL,
  `user` varchar(255) DEFAULT NULL,
  `password` varchar(255) DEFAULT NULL,
  `database` varchar(255) DEFAULT NULL,
  `active` int(2) DEFAULT NULL,
  `zone` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for task_list
-- ----------------------------
DROP TABLE IF EXISTS `task_list`;
CREATE TABLE `task_list` (
  `task_id` int(20) NOT NULL,
  `game` varchar(255) DEFAULT NULL,
  `py_name` varchar(255) DEFAULT NULL,
  `save_name` varchar(255) DEFAULT NULL,
  `unique_key` varchar(255) DEFAULT NULL,
  `day_one` int(2) DEFAULT NULL,
  `exec_tm` varchar(255) DEFAULT NULL,
  `last_tm` datetime DEFAULT NULL,
  `active` int(2) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for zone_cfg
-- ----------------------------
DROP TABLE IF EXISTS `zone_cfg`;
CREATE TABLE `zone_cfg` (
  `_id` int(10) NOT NULL,
  `zone` varchar(200) DEFAULT NULL,
  `ip` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
