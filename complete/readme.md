
This is an example for spring remote partitioning. This example reads data from a "Person" table, converts the names
to uppercase and writes them back into a "Person2" table. Same spring boot Application.java class is used for running
both salve and master nodes. There are two profiles - "master" and "slave". Certain beans are marked with
appropriate profile information so that they are initialized only in the context of running this application as master
or slave.

This application runs with MYSQL database.

I was unable to run it with HSQL DB because of database locking issue. I could not start master if a slave is
running and vice versa.

## Run master ##

Run master with following program arguments and environment variable

Program argument
--spring.batch.job.names=transformUserJob

Environment variable
spring.profiles.active=master

## Run slave ##
Run slaves with following program arguments and environment variable

Environment variable
spring.profiles.active=slave

## Run Sequence ##
This example is setup to execute all DDL statements in the schema-all.sql file on startup of slaves as well as master.
Follow any instruction on the schema-all.sql for initial setup
Since the DDL drops all tables first start all slave nodes before starting master. Master would start the batch job as
as it starts up. You do not want tables to be dropped and recreated during the batch job run.





