Pipeline to ingest GNIP Decahose into HAWQ Internal Tables using Spring-XD, PXF & PL/Python
==============================================================================================

Start each of the following components in a screen of their own.
It might be worthwhile naming the screens as singlenode, shell, decahose

Start Spring XD SNE
====================
xd-singlenode --hadoopDistro phd20 

Start Spring-XD Shell
======================
xd-shell --hadoopDistro phd20

Create a stream for the GNIP Decahose, with HTTP Source and HDFS Sink
=====================================================================
stream create --name gnipdecahose --definition "http --port=9009 | hdfs --directory=/user/vatsan/decahose/ --partitionPath=dateFormat('yyyy/MM/dd')" --deploy 

Destroying a stream
====================
If you want to close a stream, use the following:
stream destroy --name gnipdecahose

Populate Decahose on to Spring XD Source
=========================================
Now that we have set-up the Spring-XD stream with an HTTP source listening on localhost:9009
You can start the GNIP Twitter Decahose as follows (on a new screen):

python decahose.py localhost:9009

This will start ingesting the contents of the decahose stream on to the http source, which in turn will be written out to HDFS.


Viewing Stream Data on HDFS
============================
The contents of the stream will be written out to HDFS at /user/vatsan/decahose.
The folder hierarchy will be of the form YYYY/MM/DD

Starting the HAWQ-PXF-PL/Python JSON parsing Pipeline
======================================================

There are two sql files, one python file and a cron job that set-up this pipeline.
1. First set-up your .pgpass file under $HOME/.pgpass. It should be of the form: hostname:port:database:username:password
2. The very first time you set-up your environment, you should run the sql file $DECAHOSE_HOME/sql/1_parsetweetjson.sql
3. Next, set-up a cron jon to run daily. Run crontab -e and add a line like so:

```
0 0 * * * python $DECAHOSE_HOME/python/decahose_load_cron.py $DECAHOSE_HOME/sql/2_fetch_from_ext_table.sql hdm2 decahose vatsan $HOME/logs/decahose_cron.log
```

This basically runs the sql file $DECAHOSE_HOME/sql/2_fetch_from_ext_table.sql through the python wrapper $DECAHOSE_HOME/python/decahose_load_cron.py
at midnight, every day.


Authors
========
Srivatsan Ramanujam <sramanujam@gopivotal.com>
Ronert Obst <robst@gopivotal.com>
