Perfeito — aqui vai um pacote de expectations nível empresa (pronto pra plugar no seu Silver/Gold) com regras reais: qualidade, duplicidade e fraude.

🧠 Estratégia (resumo)
🥉 Bronze → sem regras (raw)
🥈 Silver → limpeza + validação
🥇 Gold → regras de negócio / fraude


🥈 SILVER — Qualidade + Duplicidade

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="silver_sales")

# 🔹 Qualidade básica
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_product", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_price", "price > 0")
@dp.expect_or_drop("valid_quantity", "quantity > 0")

# 🔹 Data válida
@dp.expect_or_drop("valid_date", "order_date IS NOT NULL")

# 🔹 Duplicidade (order_id único)
@dp.expect("unique_order", "order_id IS NOT NULL")

def silver_sales():
    
    customers = spark.read.table("customers")
    products  = spark.read.table("products")
    orders    = spark.read.table("orders")

    df = orders \
        .join(customers, "customer_id") \
        .join(products, "product_id")

    # remover duplicados (regra real)
    df = df.dropDuplicates(["order_id"])

    return df
	
	
	
🥇 GOLD — Regras de Negócio + Fraude
@dp.materialized_view(name="gold_sales")

# 🔹 Receita não negativa
@dp.expect_or_fail("valid_revenue", "total_value >= 0")

# 🔹 Limite de fraude (ticket muito alto)
@dp.expect("suspicious_high_value", "total_value < 10000")

# 🔹 Quantidade absurda
@dp.expect("suspicious_quantity", "quantity < 100")

def gold_sales():
    
    df = spark.read.table("silver_sales")

    df = df.withColumn(
        "total_value",
        F.col("quantity") * F.col("price")
    )

    return df
	
	
	
	
🚨 OPCIONAL (NÍVEL AVANÇADO) — Quarentena

Separar dados suspeitos:

@dp.table(name="fraud_orders")
def fraud_orders():
    
    df = spark.read.table("silver_sales")

    return df.filter(
        (F.col("quantity") > 100) |
        (F.col("price") > 10000)
    )
	
	
	
🔥 Boas práticas reais
	✔ Use nomes claros
	valid_*
	suspicious_*
	fraud_*
	✔ Tipos de regra
	expect → monitora
	expect_or_drop → limpa
	expect_or_fail → crítico
	✔ Onde aplicar
	Silver → limpeza pesada
	Gold → regras de negócio


🧠 Resumo
	expectations = data quality + regras reais
	evitam lixo no BI
	ajudam a detectar fraude
	padrão usado em empresas