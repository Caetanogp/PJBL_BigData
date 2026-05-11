#!/usr/bin/env bash
  set -euo pipefail

  echo "==> Iniciando pipeline PJBL_BigData"

  INPUT_PATH="${1:-input/transactions.csv}"
  JAR_PATH="target/mapreduce-pjbl01.jar"

  if [[ ! -f "pom.xml" ]]; then
    echo "ERRO: execute este script na raiz do projeto (onde existe pom.xml)."
    exit 1
  fi

  if [[ ! -f "$INPUT_PATH" ]]; then
    echo "ERRO: arquivo de entrada nao encontrado: $INPUT_PATH"
    echo "Uso: ./run_all.sh input/transactions.csv"
    exit 1
  fi

  echo "==> Compilando com Maven"
  mvn clean package

  if [[ ! -f "$JAR_PATH" ]]; then
    echo "ERRO: jar nao gerado em $JAR_PATH"
    exit 1
  fi

  echo "==> Limpando saidas anteriores no HDFS"
  hdfs dfs -rm -r -f output/q1 output/q2 output/q3 output/q4 output/q5 output/q6 output/q7 output/q8 ||
  true

  echo "==> Executando Q1..Q8"
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q1.BrazilTransactionsJob "$INPUT_PATH" output/q1
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q2.TransactionsByYearJob "$INPUT_PATH" output/q2
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q3.TransactionsByCategoryJob "$INPUT_PATH" output/q3
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q4.TransactionsByFlowJob "$INPUT_PATH" output/q4
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q5.BrazilAverageByYearJob "$INPUT_PATH" output/q5
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q6.BrazilMinMax2016Job "$INPUT_PATH" output/q6
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q7.BrazilExportAverageByYearJob "$INPUT_PATH" output/q7
  hadoop jar "$JAR_PATH" br.pucpr.mapreduce.q8.MinMaxAmountByYearCountryJob "$INPUT_PATH" output/q8

  echo "==> Exportando resultados para pasta local results/"
  mkdir -p results
  hdfs dfs -getmerge output/q1 results/q1_brazil_transactions.txt
  hdfs dfs -getmerge output/q2 results/q2_transactions_by_year.txt
  hdfs dfs -getmerge output/q3 results/q3_transactions_by_category.txt
  hdfs dfs -getmerge output/q4 results/q4_transactions_by_flow.txt
  hdfs dfs -getmerge output/q5 results/q5_avg_brazil_by_year.txt
  hdfs dfs -getmerge output/q6 results/q6_min_max_brazil_2016.txt
  hdfs dfs -getmerge output/q7 results/q7_avg_export_brazil_by_year.txt
  hdfs dfs -getmerge output/q8 results/q8_min_max_amount_by_year_country.txt

  echo "==> Validacao rapida (Q3/Q4 sem cabecalho contaminando)"
  if grep -iE '^category[[:space:]]' results/q3_transactions_by_category.txt; then
    echo "ATENCAO: encontrou 'category' em Q3"
  else
    echo "OK: Q3 sem linha 'category'"
  fi

  if grep -iE '^flow[[:space:]]' results/q4_transactions_by_flow.txt; then
    echo "ATENCAO: encontrou 'flow' em Q4"
  else
    echo "OK: Q4 sem linha 'flow'"
  fi

  echo "==> Concluido. Arquivos gerados em results/"
  ls -lh results/*.txt
