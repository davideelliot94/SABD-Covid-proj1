#!/bin/bash

rm query1_results -rvf 2>&1
$SPARK_HOME/bin/spark-submit --class Query1 --master "local"  target/SABD_Project1.jar local
rm query2_results -rvf 2>&1
$SPARK_HOME/bin/spark-submit --class Query2 --master "local"  target/SABD_Project1.jar local


