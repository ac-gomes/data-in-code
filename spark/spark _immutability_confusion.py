# Databricks notebook source
# MAGIC %md
# MAGIC ## Confusão sobre imutabilidade no Spark - RDD e DataFrame
# MAGIC Entenda de forma clara a diferença entre variável, referência e objeto.
# MAGIC Saiba porque as transformações em Spark criam novos objetos, mantendo o princípio da imutabilidade.
# MAGIC
# MAGIC ``Imutável = uma vez criado, não pode ser alterado.``
# MAGIC
# MAGIC ``Alterações no objeto via uma referência serão visíveis por todas as variáveis que o referenciam (no caso de objetos mutáveis).``

# COMMAND ----------

# MAGIC %md
# MAGIC | Termo          | Definição simples                                          |
# MAGIC | -------------- | ---------------------------------------------------------- |
# MAGIC | **Objeto**     | Valor real armazenado na memória (ex: `"Pizza"`, DataFrame). |
# MAGIC | **Referência** | Ponteiro ou endereço que aponta para o objeto.             |
# MAGIC | **Variável**   | Nome que você usa no código para acessar o objeto.         |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Probelma 1
# MAGIC Uma variável aponta para uma referência, que aponta para um objeto.
# MAGIC
# MAGIC ``⟹ Etiqueta ⟹ Referencia ⟹ Objeto``

# COMMAND ----------


comida = "Pizza"

comida2 = comida

comida = comida[:2]

# Ver resultados
print(f"comida: {comida} \n" )

print(f"comida2:  {comida2}" )

# "Se 'comida2' é igual a 'comida', então se 'comida' mudar, 'comida2' muda também."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicação
# MAGIC - `comida[:2]` cria um novo objeto com o valor `"Pi"`.
# MAGIC
# MAGIC - `comida` agora passa a apontar para esse novo objeto.
# MAGIC
# MAGIC - Mas `comida2` ainda está apontando para o antigo `"Pizza"`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prova técnica

# COMMAND ----------


print(f"Referencia: {id(comida)}, comida: {comida} \n" )

print(f"Referencia: {id(comida2)}, comida2:  {comida2}" )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exemplo equivalente com PySpark DataFrame
# MAGIC - DataFrame em PySpark é uma camada tabular sobre RDDs, e também segue o princípio de imutabilidade

# COMMAND ----------

# DataFrame original
df = spark.createDataFrame([("🍔",), ("🍟",), ("🍕",)], ["comida"])

print(f"Referencia: {id(df)}, df: {df.take(3)}")

# COMMAND ----------

# Referência para o mesmo DataFrame
df2 = df

# Filtrar para remover a 🍕
df = df.filter("comida != '🍕'")

# Mostrar resultados
print("DataFrame df:")
df.show()

print("DataFrame df2:")
df2.show()


# COMMAND ----------

df.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explicação
# MAGIC - `df2` mantém o DataFrame original com a  `'🍕'`.
# MAGIC
# MAGIC - `df` agora é um novo DataFrame sem a `'🍕'`, criado a partir de uma transformação (filter()). Com um novo plano lógico de execução (query plan).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prova técnica com .explain()

# COMMAND ----------

print(f"Referencia: {id(df)}, df: {df.take(3)} \n")

print(f"Referencia: {id(df2)}, df2: {df2.take(3)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Referncia
# MAGIC https://docs.python.org/3/reference/datamodel.html
# MAGIC
# MAGIC https://www.pythonmorsels.com/pointers/
# MAGIC
# MAGIC `The value of some objects can change. Objects whose value can change are said to be mutable; objects whose value is unchangeable once they are created are called immutable. (The value of an immutable container object that contains a reference to a mutable object can change when the latter’s value is changed; however the container is still considered immutable, because the collection of objects it contains cannot be changed. So, immutability is not strictly the same as having an unchangeable value, it is more subtle.) An object’s mutability is determined by its type; for instance, numbers, strings and tuples are immutable, while dictionaries and lists are mutable.`
