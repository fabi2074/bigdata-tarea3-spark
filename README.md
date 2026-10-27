# Procesamiento en Batch con Apache Spark - Análisis de Fraude en Tarjetas de Crédito

**Autor:** Fabián Andrés Peña González  
**Curso:** Big Data (202016911) - UNAD  

## 1. Descripción de la Solución
Este repositorio contiene la implementación en **PySpark** (`batch_creditcard.py`) para el procesamiento en lote (Batch) y Análisis Exploratorio de Datos (EDA) del conjunto de datos público *Credit Card Fraud Detection* (`creditcard.csv` de Kaggle)[cite: 19, 20].

El script ejecuta las siguientes fases:
1. **Inicialización y Carga:** Creación de la sesión `Batch_CreditCard_Fraud_EDA`, ajuste del nivel de logs a `WARN` y lectura distribuida de `creditcard.csv` con inferencia automática de esquema (`inferSchema=True`)[cite: 27].
2. **Limpieza y Validación de Nulos:** Verificación de valores nulos en las columnas críticas (`Time`, `Amount`, `Class`) y depuración del DataFrame mediante `dropna()`[cite: 27].
3. **Análisis Exploratorio (EDA) y Transformaciones:**
   * Agrupación por clase (`0 = Legítima`, `1 = Fraude`) calculando el total de transacciones, porcentaje de participación (`Porcentaje_%`), monto promedio, monto mínimo, monto máximo y monto total acumulado[cite: 27].
   * Filtrado y ordenamiento descendente para identificar el **Top 5 de transacciones fraudulentas de mayor monto**[cite: 27].

## 2. Instrucciones de Ejecución
1. Descargar el dataset `creditcard.csv` desde [Kaggle - Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) y ubicarlo en el mismo directorio del script[cite: 19].
2. Ejecutar el script en el entorno de Ubuntu con Apache Spark mediante el comando:
   ```bash
   spark-submit batch_creditcard.py
