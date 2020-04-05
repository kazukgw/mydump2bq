DROP SCHEMA IF EXISTS example;
CREATE SCHEMA example;
USE example;

CREATE TABLE address (
  address_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  address VARCHAR(50) NOT NULL,
  address2 VARCHAR(50) DEFAULT NULL,
  district VARCHAR(20) NOT NULL,
  city_id SMALLINT UNSIGNED NOT NULL,
  postal_code VARCHAR(10) DEFAULT NULL,
  phone VARCHAR(20) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY  (address_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `address` VALUES (1,'f\'\'\nuga',NULL,'hoge',300,'','','2016-09-01 00:00:00');
INSERT INTO `address` VALUES (2,'foo',NULL,'bar',300,'','','2017-02-01 00:00:00');


CREATE TABLE staff (
  id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  address_id SMALLINT UNSIGNED NOT NULL,
  picture BLOB DEFAULT NULL,
  email VARCHAR(50) DEFAULT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  username VARCHAR(16) NOT NULL,
  password VARCHAR(40) BINARY DEFAULT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY  (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `staff` VALUES (1,'John','user',3,0x89504E470D0A1A0A0000000D494,'john.user@hoge.com',1,'john','8cb2237d0679ca88db6464eac60da96345513964','2014-02-15 00:12:12');
INSERT INTO `staff` VALUES (2,'Mike','user',3,0x89504E470D0A1A0A0000000D494,'mike.user@hgoe.com',1,'mike','8cb2237d0679ca88db6464eac60da96345513964','2016-09-15 03:44:12');


CREATE TABLE `test` (
    `t_pk` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT comment 'col t_pk',
    `t_bool` BOOLEAN NOT NULL comment 'col t_bool',
    `t_tinyint1` TINYINT(1) UNSIGNED NOT NULL comment 'col t_tineyint1',
    `t_int` INT(11) NOT NULL comment 'col t_int',
    `t_bigint` BIGINT NOT NULL comment 'col t_bigint',
    `t_float` FLOAT NOT NULL comment 'col t_float',
    `t_double` DOUBLE NOT NULL comment 'col t_double',
    `t_decimal` DECIMAL(10, 2) NOT NULL comment 'col t_decimal',
    `t_char` CHAR(5) NOT NULL comment 'col t_char',
    `t_varchar` VARCHAR(255) NOT NULL comment 'col t_varchar',
    `t_varbinary` VARBINARY(255) NOT NULL comment 'col t_varbinary',
    `t_text` TEXT NOT NULL comment 'col t_text',
    `t_enum` ENUM('a', 'b', 'c') NOT NULL comment 'col t_enum',
    `t_datetime` DATETIME NOT NULL comment 'col t_datetime',
    `t_date` DATE NOT NULL comment 'col t_date',
    `t_time` TIME NOT NULL comment 'col t_time',
    `t_timestamp` TIMESTAMP NOT NULL comment 'col t_timestamp',
    PRIMARY KEY (`t_pk`)
) ENGINE=InnoDB comment='test table'
;

INSERT INTO `test` (
  `t_pk`,
  `t_bool`,
  `t_tinyint1`,
  `t_int`,
  `t_bigint`,
  `t_float`,
  `t_double`,
  `t_decimal`,
  `t_char`,
  `t_varchar`,
  `t_varbinary`,
  `t_text`,
  `t_enum`,
  `t_datetime`,
  `t_date`,
  `t_time`,
  `t_timestamp`
)
VALUES (
    1,
    true,
    0,
    123,
    123456789123456,
    123.4,
    123.456,
    1234.56,
    'abcde',
    'あいうえお',
    'abcdef',
    'あいうえおあいうえおあいうえお',
    'a',
    '2018-02-16 01:02:03',
    '2018-02-16',
    '01:02:03',
    '2018-02-16 01:02:03'
  )
