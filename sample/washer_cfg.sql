/*
Navicat MySQL Data Transfer

Source Server         : 本机
Source Server Version : 50173
Source Host           : localhost:3306
Source Database       : washer_cfg

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2017-10-08 23:28:09
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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of server_list
-- ----------------------------
INSERT INTO `server_list` VALUES ('1', 'test', 't01', '127.0.0.1', '3306', 'root', '123456', 'test1');
INSERT INTO `server_list` VALUES ('2', 'test', 't02', '127.0.0.1', '3306', 'root', '123456', 'test2');
INSERT INTO `server_list` VALUES ('3', 'ana', 'ana_db', '127.0.0.1', '3306', 'root', '123456', 'ana_db');

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
-- Records of task_list
-- ----------------------------
INSERT INTO `task_list` VALUES ('1', 'test', 'task1', 'test_1', null, '1', '* * * *', '2017-10-08 16:19:27', '1');
