from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, max, min, sum as _sum, round as _round

# 1. Inicializar sesión de Spark
spark = SparkSession.builder \
    .appName("Batch_CreditCard_Fraud_EDA") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el dataset desde la fuente original
print("=== 1. CARGANDO DATASET CREDITCARD.CSV ===")
df = spark.read.csv("creditcard.csv", header=True, inferSchema=True)
df.printSchema()

total_registros = df.count()
print(f"Total de transacciones cargadas: {total_registros}")

# 3. Limpieza y verificación de valores nulos
print("\n=== 2. LIMPIEZA Y VALIDACION DE NULOS ===")
df.select([count(when(col(c).isNull(), c)).alias(c) for c in ["Time", "Amount", "Class"]]).show()
df_clean = df.dropna()

# 4. Análisis Exploratorio de Datos (EDA) y Transformaciones
print("\n=== 3. DISTRIBUCION DE TRANSACCIONES (0 = Legitima, 1 = Fraude) ===")
resumen_clases = df_clean.groupBy("Class").agg(
    count("Amount").alias("Total_Transacciones"),
    _round((count("Amount") / total_registros) * 100, 4).alias("Porcentaje_%"),
    _round(avg("Amount"), 2).alias("Monto_Promedio"),
    _round(min("Amount"), 2).alias("Monto_Minimo"),
    _round(max("Amount"), 2).alias("Monto_Maximo"),
    _round(_sum("Amount"), 2).alias("Monto_Total_Acumulado")
)
resumen_clases.show()

print("\n=== 4. TOP 5 TRANSACCIONES FRAUDULENTAS DE MAYOR MONTO ===")
df_clean.filter(col("Class") == 1) \
    .select("Time", "V1", "V2", "Amount", "Class") \
    .orderBy(col("Amount").desc()) \
    .show(5)

spark.stop()