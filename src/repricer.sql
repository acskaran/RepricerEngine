-- --------------------------------------------------------
-- Host:                         localhost
-- Server version:               5.5.27 - MySQL Community Server (GPL)
-- Server OS:                    Win32
-- HeidiSQL version:             7.0.0.4053
-- Date/time:                    2013-05-22 14:49:45
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET FOREIGN_KEY_CHECKS=0 */;

-- Dumping database structure for repricer
CREATE DATABASE IF NOT EXISTS `repricer` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `repricer`;


-- Dumping structure for table repricer.asin_associations
CREATE TABLE IF NOT EXISTS `asin_associations` (
  `region` varchar(50) NOT NULL,
  `product_id` varchar(50) NOT NULL,
  `jp_product_id` varchar(50) NOT NULL,
  PRIMARY KEY (`region`,`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.commands
CREATE TABLE IF NOT EXISTS `commands` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `command` varchar(25) DEFAULT NULL,
  `metadata` varchar(250) DEFAULT NULL,
  `status` varchar(25) DEFAULT NULL,
  `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.feed_submissions
CREATE TABLE IF NOT EXISTS `feed_submissions` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `feed_file` varchar(100) NOT NULL,
  `amazon_submission_id` varchar(100) NOT NULL,
  `reprice_id` int(15) NOT NULL DEFAULT '0',
  `submitted_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `reprice_id` (`reprice_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.inventory_feeds
CREATE TABLE IF NOT EXISTS `inventory_feeds` (
  `id` int(11) NOT NULL,
  `url` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.inventory_items
CREATE TABLE IF NOT EXISTS `inventory_items` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sku` varchar(50) NOT NULL,
  `inventory_id` int(20) NOT NULL,
  `region_product` varchar(50) NOT NULL,
  `product_id` varchar(50) NOT NULL,
  `inventory_region` varchar(50) NOT NULL,
  `quantity` int(11) NOT NULL,
  `old_quantity` int(11) DEFAULT NULL,
  `price` float NOT NULL,
  `old_price` float DEFAULT NULL,
  `item_condition` int(10) NOT NULL,
  `lowest_amazon_price` float DEFAULT NULL,
  `obi` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `inventory_id_region_product` (`inventory_id`,`region_product`),
  KEY `sku` (`sku`),
  KEY `product_id_inventory_region` (`product_id`,`inventory_region`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.latest_inventory
CREATE TABLE IF NOT EXISTS `latest_inventory` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `region` varchar(10) NOT NULL,
  `inventory_id` int(20) NOT NULL,
  `total_items` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 MAX_ROWS=1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.product_blacklist
CREATE TABLE IF NOT EXISTS `product_blacklist` (
  `region` varchar(10) NOT NULL,
  `product_id` varchar(50) NOT NULL,
  `blacklist` int(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`region`,`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.product_details
CREATE TABLE IF NOT EXISTS `product_details` (
  `product_id` varchar(50) NOT NULL,
  `weight` float DEFAULT NULL,
  `used_quantity` int(11) NOT NULL DEFAULT '-1',
  `used_lowest_price` float NOT NULL DEFAULT '-1',
  `new_quantity` int(11) NOT NULL DEFAULT '-1',
  `new_lowest_price` float NOT NULL DEFAULT '-1',
  `last_refreshed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.repricer_configuration
CREATE TABLE IF NOT EXISTS `repricer_configuration` (
  `region` varchar(4) NOT NULL,
  `repricer_status` varchar(15) DEFAULT NULL,
  `formula_id` int(10) NOT NULL,
  `latest_reprice_id` int(10) NOT NULL DEFAULT '-1',
  `repricer_interval` int(10) NOT NULL DEFAULT '-1',
  `next_run` timestamp NULL DEFAULT NULL,
  `marketplace_id` varchar(50) NOT NULL,
  `seller_id` varchar(50) NOT NULL,
  `cache_refresh_interval` int(11) NOT NULL DEFAULT '-1'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.repricer_formula
CREATE TABLE IF NOT EXISTS `repricer_formula` (
  `formula_id` int(10) NOT NULL AUTO_INCREMENT,
  `quantity_limit` int(10) NOT NULL,
  `obi_quantity_limit` int(10) NOT NULL,
  `formula` varchar(100) NOT NULL,
  `obi_formula` varchar(100) NOT NULL,
  `default_weight` double NOT NULL DEFAULT '-1',
  `obi_default_weight` double NOT NULL DEFAULT '-1',
  `second_level_repricing` bit(1) NOT NULL,
  `lower_price_marigin` double DEFAULT NULL,
  `lower_limit` double DEFAULT NULL,
  `lower_limit_percent` double DEFAULT NULL,
  `upper_limit` double DEFAULT NULL,
  `upper_limit_percent` double DEFAULT NULL,
  PRIMARY KEY (`formula_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.repricer_reports
CREATE TABLE IF NOT EXISTS `repricer_reports` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `reprice_id` int(11) NOT NULL DEFAULT '0',
  `price` float NOT NULL,
  `quantity` int(11) NOT NULL,
  `inventory_item_id` int(11) unsigned NOT NULL,
  `formula_id` int(11) NOT NULL,
  `audit_trail` varchar(300) NOT NULL,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `FK_repricer_reports_repricer_status` (`reprice_id`),
  KEY `inventory_item_id` (`inventory_item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.repricer_status
CREATE TABLE IF NOT EXISTS `repricer_status` (
  `reprice_id` int(11) NOT NULL AUTO_INCREMENT,
  `region` varchar(50) NOT NULL,
  `r_status` varchar(50) NOT NULL,
  `total_scheduled` int(11) NOT NULL DEFAULT '0',
  `total_completed` int(11) NOT NULL DEFAULT '0',
  `total_repriced` int(11) NOT NULL DEFAULT '0',
  `elapsed` int(11) NOT NULL DEFAULT '0',
  `quantity_reset_to_zero` int(11) NOT NULL DEFAULT '0',
  `price_up` int(11) NOT NULL DEFAULT '0',
  `price_down` int(11) NOT NULL DEFAULT '0',
  `no_price_change` int(11) NOT NULL DEFAULT '0',
  `obi_quantity_reset_to_zero` int(11) NOT NULL DEFAULT '0',
  `obi_price_up` int(11) NOT NULL DEFAULT '0',
  `obi_price_down` int(11) NOT NULL DEFAULT '0',
  `obi_no_price_change` int(11) NOT NULL DEFAULT '0',
  `lowest_price` int(11) NOT NULL DEFAULT '0',
  `last_repriced_id` int(11) NOT NULL DEFAULT '0',
  `last_repriced` varchar(50) DEFAULT NULL,
  `reprice_rate` float NOT NULL DEFAULT '0',
  `start_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `end_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`reprice_id`),
  KEY `region` (`region`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.


-- Dumping structure for table repricer.users
CREATE TABLE IF NOT EXISTS `users` (
  `Username` varchar(50) DEFAULT NULL,
  `Password` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.
/*!40014 SET FOREIGN_KEY_CHECKS=1 */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
