# Databricks notebook source
# MAGIC %md
# MAGIC ## Lógica de Programação para Dados com Python e SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## O Computador
# MAGIC 1. Faz cálculos muito rápidos, ou seja, executa tarefas com uma velocidade inconcebível para um ser humano (no sentido lógico consciente)
# MAGIC
# MAGIC 2. Armazena muitos dados
# MAGIC
# MAGIC 3. O computador não toma decisões, não tem experiencia alguma em execução de tarefas, ou seja, não tem uma base referencial para guiar suas ações e decisões.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é Lógica de programação ?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC A **lógica de programação** é o processo de mapear todas as possibilidades e instruir o computador para que ele execute tarefas e tome decisões. Essencialmente, é a habilidade de organizar o pensamento e as instruções de forma que um computador possa entendê-las e executá-las para resolver um problema específico
# MAGIC
# MAGIC
# MAGIC Portanto, é o jeito de organizar o raciocínio para instruir o computador o que fazer e em que ordem.
# MAGIC Pense como uma receita de bolo: você precisa seguir passos claros (misturar, assar, esperar) para chegar ao resultado. A lógica de programação é justamente criar esses passos de forma que a máquina consiga entender.
# MAGIC
# MAGIC
# MAGIC Então para que serve **lógica de programação**? 
# MAGIC Para você aprender a estruturar seu raciocínio logico, materializar suas ideias em uma linguagem de programação para que o computador execute suas instruções seguindo cada passo.  
# MAGIC
# MAGIC ## Só se aprende Lógica de programação com muita prática.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Algoritimos

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Algoritmo é a lista desses passos, escrita de forma clara e sequencial, para resolver um problema. Por exemplo: os passos que compõem o modo de preparo de um bolo
# MAGIC
# MAGIC Em resumo o algoritmo é o passo a passo que orienta o computador a executar uma tarefa sem se perder.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##💡 Uma analogia simples:
# MAGIC
# MAGIC - Lógica de programação é como você decide que prato cozinhar e quais ingredientes usar.
# MAGIC
# MAGIC - Algoritmo é a receita escrita passo a passo para preparar o prato.

# COMMAND ----------

# MAGIC %md
# MAGIC # O Programador/Desenvolvedor
# MAGIC
# MAGIC 1. O desenvolverdor tem o papel fundamental de mapear todas as possibilidades e instruir o computador, em uma liguagem que ele entenda, para que ele execute tarefas e tome decisões em uma determinada ordem.
# MAGIC
# MAGIC 2. Em suma, o programador é quem dá "cérebro" ao software, traduzindo a intenção humana em uma sequência lógica e precisa de instruções que o computador pode processar sem se perder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Projeto Receita de Bolo
# MAGIC
# MAGIC 1. Objetivo → Fazer um bolo de cenoura com cobertura de chocolate
# MAGIC 2. Levantar os requisitos → Ingredientes e Utensílios
# MAGIC 3. Modo de Preparo → Algoritimo
# MAGIC
# MAGIC
# MAGIC link do projeto: https://www.tudogostoso.com.br/receita/23-bolo-de-cenoura.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício prático
# MAGIC
# MAGIC **Objetivo:** 
# MAGIC
# MAGIC Criar um novo volume na camada `bronze` chamado `exercicios`, criar um diretório chamado `arquivos` e criar um arquivo de texto com a frase "Olá mundo" com nome ``exercicio1.txt``.
# MAGIC
# MAGIC **Algoritimo:**
# MAGIC
# MAGIC 1. Criar volume
# MAGIC 2. Criar diretório
# MAGIC 3. Criar arquivo com a frase "Olá mundo"
# MAGIC
# MAGIC Referencia Tecnica:
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/volumes/utility-commands
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/files/volumes
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-connector-put-into

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE VOLUME dev.bronze.exercicios
# MAGIC
# MAGIC dbutils.fs.mkdirs("/Volumes/dev/bronze/exercicios/arquivos")
# MAGIC
# MAGIC dbutils.fs.put("/Volumes/dev/bronze/exercicios/arquivos/exercicio1.txt", "Olá mundo")
# MAGIC

# COMMAND ----------

volume_foi_criado = spark.sql("CREATE VOLUME IF NOT EXISTS dev.bronze.exercicios")


diretorio_foi_criado = dbutils.fs.mkdirs("/Volumes/dev/bronze/exercicios/arquivos")

if diretorio_foi_criado == True:
  print("Diretório criado com sucesso!\n")

  dbutils.fs.put("/Volumes/dev/bronze/exercicios/arquivos/exercicio1.txt", "Olá mundo")
  print("Arquivo criado com sucesso!")

else:
  print("Falha ao criar Arquivo!")


