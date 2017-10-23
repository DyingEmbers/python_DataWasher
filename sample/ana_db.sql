/*
Navicat MySQL Data Transfer

Source Server         : 本机
Source Server Version : 50173
Source Host           : localhost:3306
Source Database       : ana_db

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2017-10-08 23:28:21
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for test_1
-- ----------------------------
DROP TABLE IF EXISTS `test_1`;
CREATE TABLE `test_1` (
  `__idx` int(20) NOT NULL AUTO_INCREMENT,
  `id` int(20) NOT NULL,
  `val` varchar(255) DEFAULT NULL,
  `server` varchar(255) DEFAULT NULL,
  `wash_date` datetime DEFAULT NULL,
  `wash_time` datetime DEFAULT NULL,
  PRIMARY KEY (`__idx`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of test_1
-- ----------------------------
