from pyspark.sql import SparkSession

# 初始化 Spark（或连接已有 session）
spark = (
    SparkSession.builder
    .appName("CheckMemory")
    .config("spark.driver.memory", "8g")   # 你可以改成你要的值
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

# 获取 SparkContext 对象
sc = spark.sparkContext

# 输出 driver memory 参数
print("spark.driver.memory =", sc._conf.get("spark.driver.memory"))
print("spark.executor.memory =", sc._conf.get("spark.executor.memory"))

spark.stop()
