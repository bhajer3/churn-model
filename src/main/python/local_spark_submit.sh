#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
 --master local[*] \
 --py-files churn_model.py \
 churn_model.py \
