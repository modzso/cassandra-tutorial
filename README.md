cassandra-tutorial
==================

Cassandra tutorial

Setting up cassandra column families:
=====================================
1. create column family auth_failures WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;
2. create column family cassandra_test WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;

All class assumes cassandra node is active on localhost, default port: 9160, keyspace: hr

Build:
======

mvn clean install
