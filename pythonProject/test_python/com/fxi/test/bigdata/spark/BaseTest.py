from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .master("spark://192.168.0.147:7077") \
    .appName("python") \
    .config("spark.executor.memory", "20g")\
    .config("spark.jars", "/Users/seki/.m2/repository/mysql/mysql-connector-java/5.1.34/mysql-connector-java-5.1.34.jar") \
    .config("spark.driver.extraClassPath", "/Users/seki/.m2/repository/mysql/mysql-connector-java/5.1.34/mysql-connector-java-5.1.34.jar") \
    .getOrCreate()


nums = spark.sparkContext.parallelize(xrange(1000000))
print nums.count()

#dataframes


url = "jdbc:mysql://122.144.134.67:3306/servicedb"
user = "huaqianv3"
psw = "huaqianv3"



jdbcDF2 = spark.read \
    .jdbc(url=url, table="ml_func_stat_ad_download", predicates=["day >= 20170105 and adId != 'all-new' "],
          properties={"user": user, "password": psw}).cache()

print jdbcDF2.count()

print jdbcDF2.first()

print jdbcDF2.select("day", "adId").distinct().count()

