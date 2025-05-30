# Databricks notebook source
# MAGIC %md
# MAGIC # 🪐 Projeto - Pipeline Bronze , Silver e Gold no Databricks
# MAGIC
# MAGIC 🥉 A camada Bronze consiste em **armazenar os arquivos brutos exatamente como recebidos**, sem qualquer tratamento, limpeza ou parsing.  
# MAGIC
# MAGIC 🥈 A camada Silver consiste no tratamento e padronização dos arquivos, para facilitar a sua leitura.
# MAGIC
# MAGIC 🥇 A camada Gold consiste na visualização dos dados através de gráficos e os respectivos dados ja tratado na camada silver.
# MAGIC
# MAGIC - O objetivo aqui é **preservar a integridade dos dados originais**, sem alterá-los.  
# MAGIC - No Databricks, utilizamos o método `text()` para armazenar os dados como linhas de texto puro, mantendo os acentos e caracteres como estão no arquivo de origem.

# COMMAND ----------

# 🥉 Bronze - Exibir o conteúdo bruto do arquivo, sem tratamento

# Leitura de cada arquivo como texto (linha a linha, sem parsear CSV)
df_atendimentos_bronze = spark.read.text("dbfs:/FileStore/tables/atendimentos.csv")
df_cirurgias_bronze = spark.read.text("dbfs:/FileStore/tables/cirurgias_sus.csv")
df_exames_bronze = spark.read.text("dbfs:/FileStore/tables/exames.csv")

# Mostrar uma amostra das linhas brutas (cada linha como uma string)
print("Atendimentos - Bronze (dados brutos):")
df_atendimentos_bronze.show(5, truncate=False)

print("Cirurgias - Bronze (dados brutos):")
df_cirurgias_bronze.show(5, truncate=False)

print("Exames - Bronze (dados brutos):")
df_exames_bronze.show(5, truncate=False)

# Copiar os arquivos originais para a camada Bronze, como foram recebidos
dbutils.fs.cp("dbfs:/FileStore/tables/atendimentos.csv", "dbfs:/mnt/bronze/atendimentos.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/cirurgias_sus.csv", "dbfs:/mnt/bronze/cirurgias_sus.csv")
dbutils.fs.cp("dbfs:/FileStore/tables/exames.csv", "dbfs:/mnt/bronze/exames.csv")


# COMMAND ----------

# 🥈 Silver - leitura formatada dos CSVs
path_atendimentos_bronze = "dbfs:/mnt/bronze/atendimentos.csv"
path_cirurgias_bronze = "dbfs:/mnt/bronze/cirurgias_sus.csv"
path_exames_bronze = "dbfs:/mnt/bronze/exames.csv"

# Leitura formatada
df_atendimentos = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .load(path_atendimentos_bronze)
)

df_cirurgias = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .load(path_cirurgias_bronze)
)

df_exames = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .load(path_exames_bronze)
)

# Visualização já formatada
print("Atendimentos - Silver:")
display(df_atendimentos.limit(100))
print("Cirurgias - Silver:")
display(df_cirurgias.limit(100))
print("Exames - Silver:")
display(df_exames.limit(100))

# Salvar em Parquet
df_atendimentos.write.mode("overwrite").format("parquet").save("/mnt/silver/atendimentos")
df_cirurgias.write.mode("overwrite").format("parquet").save("/mnt/silver/cirurgias")
df_exames.write.mode("overwrite").format("parquet").save("/mnt/silver/exames")

# COMMAND ----------

# 🥇 Gold - Preparação para Visualização dos dados de 'atendimentos'

# IMPORTA AS BIBLIOETAS NECESSARIAS
from pyspark.sql.functions import year, month, count, col, to_date, regexp_replace, trim, when, lit

#  Carrega os dados da camada Silver (Parquet)
print("Carregando dados de atendimentos da camada Silver (Parquet)...")
df_atendimentos_silver = spark.read.format("parquet").load("/mnt/silver/atendimentos")
print("CARREGADO COM SUCESSO!")

# Garente que todos os nomes das colunas esteja certo.
print("\nEsquema do DataFrame de Atendimentos (Silver) para referência:")
df_atendimentos_silver.printSchema()

#Transformações e Agregações para a Camada Gold 
print("\nRealizando transformações e agregações para a camada Gold de atendimentos...")

# Limpeza e Agregação por 'local_atendimento'
PADROES_PARA_REMOVER_LOCAL = "(?i)(S/A| - EI|\\.{3}|Rua|Av\\.|Trav\\.|Avenida|Praca|Praça|Rodovia|Rod)" # <-- AJUSTE ESTA EXPRESSÃO REGULAR

# Aplicando a limpeza diretamente na coluna 'local_atendimento'
df_atendimentos_gold = df_atendimentos_silver.withColumn(
    "local_atendimento", 
    regexp_replace(
        col("local_atendimento"),
        PADROES_PARA_REMOVER_LOCAL,
        ""
    )
)

df_atendimentos_gold = df_atendimentos_gold.withColumn(
    "local_atendimento",
    trim(regexp_replace(col("local_atendimento"), "\\.$", "")) 
)
#  FINALIZANDO A LIMPEZA NA COLUNA ORIGINAL
df_atendimentos_gold = df_atendimentos_gold.withColumn(
    "local_atendimento",
    when(col("local_atendimento").isNull() | (trim(col("local_atendimento")) == ""), lit("Nao Informado"))
    .otherwise(col("local_atendimento"))
)

# Agrupa e conta usando a coluna 'local_atendimento' já limpa
df_atendimentos_gold_por_local = df_atendimentos_gold \
    .groupBy("local_atendimento") \
    .agg(count("*").alias("TotalAtendimentos")) \
    .orderBy(col("TotalAtendimentos").desc())


# Agregação por 'data_atendimento' (Mes/Ano) 
NOME_COLUNA_DATA_ATENDIMENTO = "data_atendimento" 
FORMATO_DATA_ATENDIMENTO = "yyyy-MM-dd" 

df_atendimentos_com_data_atendimento_formatada = df_atendimentos_silver.withColumn(
    "data_atendimento_formatada", to_date(col(NOME_COLUNA_DATA_ATENDIMENTO), FORMATO_DATA_ATENDIMENTO)
)

df_atendimentos_gold_por_data_atendimento = df_atendimentos_com_data_atendimento_formatada \
    .filter(col("data_atendimento_formatada").isNotNull()) \
    .groupBy(year("data_atendimento_formatada").alias("Ano"), \
             month("data_atendimento_formatada").alias("Mes")) \
    .agg(count("*").alias("TotalAtendimentos")) \
    .orderBy("Ano", "Mes")

# Agregação por 'medico'

NOME_COLUNA_MEDICO = "medico"

df_atendimentos_gold_por_medico = df_atendimentos_silver \
    .groupBy(col(NOME_COLUNA_MEDICO).alias("NomeMedico")) \
    .agg(count("*").alias("TotalAtendimentos")) \
    .orderBy(col("TotalAtendimentos").desc())

print("Agregações para gráficos de atendimentos concluídas.")

# Salva os dados agregados para o grafico na Camada Gold 
print("\nSalvando DataFrames agregados de atendimentos na camada Gold (Parquet)...")
df_atendimentos_gold_por_local.write.mode("overwrite").format("parquet").save("/mnt/gold/atendimentos_por_local")
df_atendimentos_gold_por_data_atendimento.write.mode("overwrite").format("parquet").save("/mnt/gold/atendimentos_por_data_atendimento")
df_atendimentos_gold_por_medico.write.mode("overwrite").format("parquet").save("/mnt/gold/atendimentos_por_medico")
print("Salvos com sucesso.")


#  Cria visualizaçoes temporárias para os gráficos
print("\nCriando temporárias a partir dos DataFrames da camada Gold...")
df_atendimentos_gold_por_local.createOrReplaceTempView("gold_atendimentos_por_local")
df_atendimentos_gold_por_data_atendimento.createOrReplaceTempView("gold_atendimentos_por_data_atendimento")
df_atendimentos_gold_por_medico.createOrReplaceTempView("gold_atendimentos_por_medico")
print("CRIADAS: 'gold_atendimentos_por_local', 'gold_atendimentos_por_data_atendimento', 'gold_atendimentos_por_medico'.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT local_atendimento, TotalAtendimentos
# MAGIC FROM gold_atendimentos_por_local
# MAGIC ORDER BY TotalAtendimentos DESC
# MAGIC LIMIT 20 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Ano, Mes, TotalAtendimentos
# MAGIC FROM gold_atendimentos_por_data_atendimento
# MAGIC ORDER BY Ano, Mes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NomeMedico, TotalAtendimentos
# MAGIC FROM gold_atendimentos_por_medico
# MAGIC ORDER BY TotalAtendimentos DESC
# MAGIC LIMIT 10