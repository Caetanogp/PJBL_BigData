Integrantes: Davi Henrique, Caetano Padoin e Matheus Brehm

# PjBL01 — MapReduce (Big Data — PUCPR)

Soluções Hadoop MapReduce em Java para responder às 8 perguntas do projeto,`r`nprocessando um dataset CSV
de transações comerciais internacionais (10 colunas, separador `;`).

## Pré-requisitos

- Java 8 ou superior
- Maven 3.6+
- Apache Hadoop 3.3.6 (para execução)

## Estrutura

```
PJBL_BigData/
├── pom.xml
├── input/                      # coloque aqui o arquivo transactions.csv
├── output/                     # diretórios de saída do Hadoop (um por questão)
├── results/                    # arquivos .txt finais (um por questão)
└── src/main/java/br/pucpr/mapreduce/
    ├── common/                 # classes reutilizáveis
    │   ├── CsvParser.java
    │   ├── Transaction.java
    │   ├── AvgWritable.java        (Writable customizado — Q5, Q7)
    │   ├── MinMaxWritable.java     (Writable customizado — Q6, Q8)
    │   └── YearCountryKey.java     (WritableComparable — Q8)
    ├── q1/  q2/  q3/  q4/      # jobs de contagem
    ├── q5/  q7/                # médias com AvgWritable
    ├── q6/                     # min/max Brasil 2016
    └── q8/                     # min/max Amount por ano e país
```

## Dataset

Coloque o CSV em `input/transactions.csv`. O parser:

- Ignora a primeira linha (cabeçalho).
- Valida que cada linha tem exatamente 10 colunas.
- Trata valores numéricos vazios ou inválidos sem quebrar o job.

## Compilação

```powershell
mvn clean package
```

Gera `target/mapreduce-pjbl01.jar`.

## Execução de cada questão

Padrão geral:

```bash
hadoop jar target/mapreduce-pjbl01.jar <CLASSE_PRINCIPAL> input/transactions.csv output/qN
```

| # | Pergunta | Classe principal | Saída sugerida |
|---|---|---|---|
| Q1 | Nº transações Brasil | `br.pucpr.mapreduce.q1.BrazilTransactionsJob` | `output/q1` |
| Q2 | Nº transações por ano | `br.pucpr.mapreduce.q2.TransactionsByYearJob` | `output/q2` |
| Q3 | Nº transações por categoria | `br.pucpr.mapreduce.q3.TransactionsByCategoryJob` | `output/q3` |
| Q4 | Nº transações por fluxo | `br.pucpr.mapreduce.q4.TransactionsByFlowJob` | `output/q4` |
| Q5 | Média de Price por ano (Brasil) | `br.pucpr.mapreduce.q5.BrazilAverageByYearJob` | `output/q5` |
| Q6 | Min/Max Price Brasil 2016 | `br.pucpr.mapreduce.q6.BrazilMinMax2016Job` | `output/q6` |
| Q7 | Média de Price exportações Brasil/ano | `br.pucpr.mapreduce.q7.BrazilExportAverageByYearJob` | `output/q7` |
| Q8 | Min/Max Amount por ano+país | `br.pucpr.mapreduce.q8.MinMaxAmountByYearCountryJob` | `output/q8` |

Exemplo:

```bash
hadoop jar target/mapreduce-pjbl01.jar br.pucpr.mapreduce.q1.BrazilTransactionsJob input/transactions.csv output/q1
```

> O Hadoop **não permite que o diretório de saída exista**. Apague `output/qN`
> antes de re-executar uma questão.

## Coleta dos resultados em `.txt`

Após cada execução, copie o arquivo de saída para `results/`:

```powershell
Copy-Item output/q1/part-r-00000 results/q1_brazil_transactions.txt
Copy-Item output/q2/part-r-00000 results/q2_transactions_by_year.txt
Copy-Item output/q3/part-r-00000 results/q3_transactions_by_category.txt
Copy-Item output/q4/part-r-00000 results/q4_transactions_by_flow.txt
Copy-Item output/q5/part-r-00000 results/q5_avg_brazil_by_year.txt
Copy-Item output/q6/part-r-00000 results/q6_min_max_brazil_2016.txt
Copy-Item output/q7/part-r-00000 results/q7_avg_export_brazil_by_year.txt
Copy-Item output/q8/part-r-00000 results/q8_min_max_amount_by_year_country.txt
```
