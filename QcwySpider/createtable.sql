-- ----------------------------
-- Table structure for source_qcwy
-- ----------------------------
DROP TABLE IF EXISTS `source_qcwy`;
CREATE TABLE `source_qcwy` (
  `zwmc` varchar(50) DEFAULT NULL,
  `fkl` varchar(50) DEFAULT NULL,
  `gsmc` varchar(50) DEFAULT NULL,
  `zwyx` varchar(50) DEFAULT NULL,
  `gzdd` varchar(50) DEFAULT NULL,
  `zwmcurl` varchar(100) DEFAULT NULL,
  `fbrq` varchar(50) DEFAULT NULL,
  `fl` varchar(200) DEFAULT NULL,
  `zwlb` varchar(50) DEFAULT NULL,
  `gzjy` varchar(50) DEFAULT NULL,
  `zdxl` varchar(50) DEFAULT NULL,
  `zprs` varchar(50) DEFAULT NULL,
  `zwms` varchar(8000) DEFAULT NULL,
  `cjsj` datetime DEFAULT NULL,
  `source` varchar(30) DEFAULT NULL,
  `level` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



-- ----------------------------
-- Table structure for source_qcwy_errlog
-- ----------------------------
DROP TABLE IF EXISTS `source_qcwy_errlog`;
CREATE TABLE `source_qcwy_errlog` (
  `msg` varchar(500) DEFAULT NULL,
  `err` varchar(500) DEFAULT NULL,
  `sj` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;