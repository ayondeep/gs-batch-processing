-- HSQLDB
--DROP TABLE person IF EXISTS ;
--DROP TABLE  person2 IF EXISTS;

--CREATE TABLE person  (
--    person_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
--    first_name VARCHAR(20),
--    last_name VARCHAR(20)
--);
--
--CREATE TABLE person2  (
--    person_id BIGINT,
--    first_name VARCHAR(20),
--    last_name VARCHAR(20)
--);

-- MYSQL
-- Uncomment following two statements only if you want to initialize person table for initial setup
-- DROP TABLE  IF EXISTS person;
--CREATE TABLE person  (
--    person_id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
--    first_name VARCHAR(20),
--    last_name VARCHAR(20)
--);

DROP TABLE  IF EXISTS person2;
CREATE TABLE person2  (
    person_id BIGINT,
    first_name VARCHAR(20),
    last_name VARCHAR(20)
);
