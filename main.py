from matplotlib import pyplot as plt
from pyspark import SparkConf
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql.connect.functions import date_format
from pyspark.sql.functions import col, unix_timestamp, avg, round, count, datediff, avg, when, coalesce, hour, minute, sum, to_date, round
from pyspark.sql.types import TimestampType

spark1 = SparkSession.builder.appName("Spark1") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
df = spark.read.csv('missile_attacks_daily.csv', header=True, inferSchema=True)
# df.show()
# df = spark.read.csv('missiles_and_uav.csv', header=True, inferSchema=True)
# df.show()
# df = spark.read.csv('oblasts_only.csv', header=True, inferSchema=True)
# df.show()

data = spark1.read.csv("missile_attacks_daily.csv", header=True, inferSchema=True)
#
# data = data.withColumn("time_start", unix_timestamp("time_start", "yyyy-MM-dd H:mm").cast(TimestampType()))
# data = data.withColumn("time_end", unix_timestamp("time_end", "yyyy-MM-dd H:mm").cast(TimestampType()))
#
# data = data.withColumn("attack_duration_minutes", (col("time_end").cast("long") - col("time_start").cast("long")) / 60)
#
# average_duration = data.agg(avg("attack_duration_minutes")).collect()[0][0]
#
# print("Середня тривалість атаки: {:.2f} хвилин".format(average_duration))
#
# spark1.stop()


# ---------------------бізнес питання №1-------------------------

data = data.withColumn("ratio", (col("destroyed") / col("launched")) * 100)
percentage_destroyed = data.groupBy("model").agg(
    round(avg(col("ratio")), 2).alias("Збито (%)")
).orderBy(col("Збито (%)").desc())
total_launched = data.groupBy("model").agg(
    sum("launched").alias("Загальна кількість випущених боєприпасів")
)
result = percentage_destroyed.join(total_launched, on="model").orderBy(col("Збито (%)").desc())
missiles_uav_data = spark.read.csv("missiles_and_uav.csv", header=True, inferSchema=True)
missiles_uav_data = missiles_uav_data.withColumnRenamed("category", "Категорія боєприпасу")
data_with_category = data.join(missiles_uav_data.select("model", "Категорія боєприпасу"), on="model", how="left")
data_with_category = data_with_category.withColumn("ratio", (col("destroyed") / col("launched")) * 100)
percentage_destroyed = data_with_category.groupBy("model", "Категорія боєприпасу").agg(
    round(avg(col("ratio")), 2).alias("Збито (%)")
).orderBy(col("Збито (%)").desc())
total_launched = data_with_category.groupBy("model", "Категорія боєприпасу").agg(
    sum("launched").alias("Загальна кількість випущених боєприпасів")
)

# ---------------------бізнес питання №2-------------------------


model_launch_counts = data.groupBy("model").agg({"launched": "sum"}).withColumnRenamed("sum(launched)",
                                                                                       "Всього випущено")

model_launch_counts_pd = model_launch_counts.toPandas()

plt.figure(figsize=(12, 6))
plt.bar(model_launch_counts_pd["model"], model_launch_counts_pd["Всього випущено"])
plt.xlabel("Модель боєголовки")
plt.ylabel("Кількість випущених боєголовок")
plt.title("Кількість випущених боєголовок за моделями")
plt.xticks(rotation=90)

for index, value in enumerate(model_launch_counts_pd["Всього випущено"]):
    plt.text(index, value, str(value), ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.show()
#
# # ---------------------бізнес питання №3-------------------------
#
#
data = data.withColumn("Дата", to_date(col("time_start")))
data = data.withColumn("date_end", to_date(col("time_end")))

data = data.withColumn("damage_diff", col("launched") - col("destroyed"))

unharmed_days = data.groupBy("Дата").agg({"damage_diff": "sum"}).withColumnRenamed("sum(damage_diff)", "Незбито")

sorted_unharmed_days = unharmed_days.orderBy(col("Незбито").desc())
sorted_unharmed_days.show()

first_place = sorted_unharmed_days.limit(1).select("Дата").collect()[0][0]
data_for_first_place = data.filter(data["time_start"].contains(first_place))
data_for_first_place.show(truncate=False)
#
# # ---------------------бізнес питання №4-------------------------
#
month_names = {
    "January": "Січень",
    "February": "Лютий",
    "March": "Березень",
    "April": "Квітень",
    "May": "Травень",
    "June": "Червень",
    "July": "Липень",
    "August": "Серпень",
    "September": "Вересень",
    "October": "Жовтень",
    "November": "Листопад",
    "December": "Грудень"
}

data = data.withColumn("Місяць", date_format(col("time_start"), "MMMM"))

data = data.replace(month_names, subset="Місяць")

launcher_months = data.groupBy("Місяць").agg({"launched": "sum"}).withColumnRenamed("sum(launched)",
                                                                                    "Випущено ракет/дронів")

sorted_months = launcher_months.orderBy(col("Випущено ракет/дронів").desc())

sorted_months.show(truncate=False)

data = data.withColumn("date_start", to_date(col("time_start")))
data = data.withColumn("date_end", to_date(col("time_end")))

data = data.withColumn("damage_diff", col("launched") - col("destroyed"))

harm_days = data.groupBy("date_start").agg({"damage_diff": "sum"}).withColumnRenamed("sum(damage_diff)", "Незбито")

sorted_harm_days = harm_days.orderBy(col("total_undestroyed").desc())

sorted_harm_days.show()


# -------------------бізнес питання №5 ---------------------------------

data = data.withColumn("Дата", to_date(col("time_start")))
data = data.withColumn("кінець_обстрілу", to_date(col("time_end")))

days = data.groupBy("Дата").agg({"launched": "sum"}).withColumnRenamed("sum(launched)", "Випущено ракет/дронів")

sorted_days = days.orderBy(col("Випущено ракет/дронів").desc())

sorted_days.show()

# ---------------------бізнес питання №6-------------------------

df = df.withColumn("started_at", unix_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ssX").cast("timestamp"))
df = df.withColumn("finished_at", unix_timestamp(col("finished_at"), "yyyy-MM-dd HH:mm:ssX").cast("timestamp"))

df = df.withColumn("Тривалість", (col("finished_at").cast(
    t.DoubleType()) - col("started_at").cast(t.DoubleType())) / 60)

average_duration_per_oblast = df.groupBy("oblast").avg("Тривалість")

average_duration_per_oblast = average_duration_per_oblast.withColumnRenamed("avg(Тривалість)", "Середня тривалість")

average_duration_per_oblast = average_duration_per_oblast.withColumn("Середня тривалість",
                                                                     round("Середня тривалість", 2))

average_duration_per_oblast.show(average_duration_per_oblast.count(), truncate=False)

# ---------------------бізнес питання №7-------------------------

official_data = spark.read.csv("official_data_en.csv", header=True, inferSchema=True)

data_for_first_place = official_data.filter(col("started_at").cast("date") == first_place)

data_for_first_place.show(truncate=False)

# ---------------------бізнес питання №8-------------------------

official_data = spark.read.csv("official_data_en.csv", header=True, inferSchema=True)

data_for_first_place = official_data.filter(col("started_at").cast("date") == first_place)

data_for_first_place = data_for_first_place.withColumn("started_time", date_format("started_at", "HH:mm"))
data_for_first_place = data_for_first_place.withColumn("finished_time", date_format("finished_at", "HH:mm"))
data_for_first_place.select("oblast", "started_time", "finished_time").show(data_for_first_place.count(),
                                                                            truncate=False)
