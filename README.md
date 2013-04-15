cassandra-tutorial
==================

Cassandra tutorial

Setting up cassandra column families:
=====================================

From Cassandra CLI:
-------------------

1. create column family auth_failures WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;
2. create column family cassandra_test WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;
3. create column family Files with comparator = 'CompositeType(UTF8Type, UTF8Type, UTF8Type)' and key_validation_class = 'UTF8Type' and default_validation_class = 'UTF8Type';

All class assumes cassandra node is active on localhost, default port: 9160, keyspace: hr

Build:
======
    mvn clean install assembly:single

Run
===

    java -cp target/cassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar cassandra.AstyanaxCountingDao
    java -cp target/cassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar cassandra.HectorCountingDao
    java -cp target/cassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar cassandra.DaoUser
    java -cp target/cassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar cassandra.composite.CompositeDao


Requirements:

- maven
- jdk 7
- cassandra cluster


Upcoming:
---------

- Composite columns
- Entity persister