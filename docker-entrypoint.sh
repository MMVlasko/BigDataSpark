#!/bin/bash

run_master() {
  echo "Starting Spark Master on spark-master:7077..."
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port 7077 \
    --webui-port 8080
}

run_worker() {
  echo "Starting Spark Worker connecting to ${SPARK_MASTER_URL}..."
  exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port 8081 \
    ${SPARK_MASTER_URL}
}

run_etl() {
  echo "========================================="
  echo "Starting ETL Pipeline..."
  echo "========================================="

  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.sql.legacy.timeParserPolicy=LEGACY \
    /opt/spark-apps/main.py

  echo "========================================="
  echo "ETL Pipeline completed!"
  echo "========================================="

  tail -f /dev/null
}

case "$SPARK_MODE" in
  master)
    run_master
    ;;
  worker)
    run_worker
    ;;
  etl)
    run_etl
    ;;
  *)
    echo "Error: SPARK_MODE must be 'master', 'worker', or 'etl'"
    echo "Current value: ${SPARK_MODE}"
    exit 1
    ;;
esac