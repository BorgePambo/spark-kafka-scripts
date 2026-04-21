from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ========================= com surrogate
# PATHS
# =========================
silver_path = "abfss://container@storage.dfs.core.windows.net/silver/customer"
gold_path   = "abfss://container@storage.dfs.core.windows.net/gold/dim_customer"

# =========================
# 1. READ + DEDUP SILVER
# =========================
silver_df = spark.read.format("delta").load(silver_path)

window_spec = Window.partitionBy("customer_id").orderBy(F.col("record_date").desc())

silver_df = (
    silver_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

# =========================
# 2. STAGING + HASH (SCD2 CONTROL)
# =========================
staging_df = (
    silver_df.select(
        "customer_id",
        "customer_name",
        "customer_email",
        "customer_phone"
    )
    .withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws("||",
                F.lower(F.trim("customer_name")),
                F.lower(F.trim("customer_email")),
                F.col("customer_phone")
            ), 256
        )
    )
    .withColumn("effective_date", F.current_timestamp())
    .withColumn("end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .withColumn("ingestion_date", F.current_timestamp())
)

# =========================
# 3. CREATE GOLD TABLE IF NOT EXISTS
# =========================
if not DeltaTable.isDeltaTable(spark, gold_path):
    (
        staging_df
        .withColumn("customer_sk", F.monotonically_increasing_id())
        .write.format("delta")
        .save(gold_path)
    )

gold_table = DeltaTable.forPath(spark, gold_path)

# =========================
# 4. SCD2 MERGE (ENTERPRISE PATTERN)
# =========================
source = staging_df.alias("s")
target = gold_table.alias("t")

# STEP 1: CLOSE OLD RECORDS
target.merge(
    source,
    """
    t.customer_id = s.customer_id
    AND t.is_current = true
    """
).whenMatchedUpdate(
    condition="t.row_hash <> s.row_hash",
    set={
        "end_date": "s.effective_date",
        "is_current": F.lit(False)
    }
).execute()

# STEP 2: INSERT NEW VERSION
gold_table.alias("t").merge(
    source,
    """
    t.customer_id = s.customer_id
    AND t.is_current = true
    AND t.row_hash = s.row_hash
    """
).whenNotMatchedInsert(values={
    "customer_sk": F.monotonically_increasing_id(),
    "customer_id": "s.customer_id",
    "customer_name": "s.customer_name",
    "customer_email": "s.customer_email",
    "customer_phone": "s.customer_phone",
    "row_hash": "s.row_hash",
    "effective_date": "s.effective_date",
    "end_date": "s.end_date",
    "is_current": "s.is_current",
    "ingestion_date": "s.ingestion_date"
}).execute()

# =========================
# 5. OPTIMIZATION (OPTIONAL)
# =========================
spark.sql(f"""
OPTIMIZE delta.`{gold_path}`
ZORDER BY (customer_id)
""")




--------------------------------------------
CDF (Change Data Feed) no Delta Lake — resumo:

👉 Ele é aplicado na tabela, sim.

O que ele faz:
Ativa um log de mudanças na tabela
Registra automaticamente:
inserts (novas linhas)
updates (linhas alteradas)
deletes (linhas removidas)
Em uma frase:

👉 O CDF faz a tabela “guardar o histórico das mudanças”, sem você precisar fazer SCD2 manual.

Diferença rápida:
SCD2 → você cria novas linhas manualmente no MERGE
CDF → o Delta já registra as mudanças automaticamente para você consultar depois


No Delta Lake, você aplica o CDF (Change Data Feed) diretamente na tabela, ativando uma propriedade.

1. Ativar o CDF na criação da tabela
CREATE TABLE customer (
  id INT,
  name STRING,
  city STRING
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);


2. Ou ativar depois (tabela já existe)
ALTER TABLE customer
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

3. Como usar depois (ler mudanças)

Você consulta as mudanças assim:

df = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .table("customer")

O que você vai ver:
_change_type → insert / update_preimage / update_postimage / delete
_commit_version → versão da mudança]

Resumo simples:

👉 Você ativa o CDF na tabela Delta e depois consegue ler todas as mudanças (inserts, updates e deletes) de forma automática.