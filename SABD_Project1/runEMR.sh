#!/bin/bash


hdfs dfs -rm -R /user/hadoop/query2_results
spark-submit --class Query2  target/SABD_Project1.jar EMR

hdfs dfs -rm -R /user/hadoop/query1_results
spark-submit --class Query1  target/SABD_Project1.jar EMR


