
# Инициализация SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min

spark = SparkSession.builder.appName("BetFiltering").getOrCreate()

#Загрузка данных из CSV
bets_df = spark.read.csv("/mnt/data/bets.csv", header=True, inferSchema=True)
events_df = spark.read.csv("/mnt/data/events.csv", header=True, inferSchema=True)
# bets_df.show(5) # Для просмотра промежуточного результата
# events_df.show(5) # Для просмотра промежуточного результата

#Соединение ставок с событиями и фильтрация по "Киберспорту"
esports_bets = (bets_df
    .join(events_df, bets_df["event_id"] == events_df["event_id"], "left")
    .filter(col("sport") == "E-Sports")
)
# esports_bets.show(5) # Для просмотра промежуточного результата

# Фильтрация по дате приёма и расчёта ставки, а также стадии "Prematch"
esports_bets = esports_bets.filter(
    (col("accept_time") >= "2022-03-14 12:00:00") &
    (col("settlement_time") <= "2022-03-15 12:00:00") &
    (col("event_stage") == "Prematch")
)
# esports_bets.show(5) # Для просмотра промежуточного результата

# Исключение ставок типа "System", Cashout, Return и FreeBet
esports_bets = esports_bets.filter(
    (col("bet_type") != "System") &
    (col("is_free_bet") == False) &
    (~col("item_result").like("Return%")) &
    (col("item_result") != "Cashout")
)
# esports_bets.show(5) # Для просмотра промежуточного результата

# Определение игроков с минимальным коэффициентом >= 1.5 для ставок "Express"
express_players = (esports_bets
    .filter((col("bet_type") == "Express") & (col("accepted_bet_odd") >= 1.5))
    .groupBy("player_id")
    .agg(spark_min("accepted_bet_odd").alias("min_odd"))
    .filter(col("min_odd") >= 1.5)
    .select("player_id")
    .rdd.flatMap(lambda x: x)
    .collect()
)

# Фильтрация по типу ставок и коэффициенту
filtered_bets = esports_bets.filter(
    ((col("bet_type") == "Ordinar") & (col("accepted_bet_odd") >= 1.5)) |
    ((col("bet_type") == "Express") & (col("accepted_bet_odd") >= 1.5) &
     col("player_id").isin(express_players))
)

# Финальная фильтрация по сумме и выборка уникальных player_id
final_bets = filtered_bets.filter(col("amount") >= 10).select("player_id").distinct()

final_bets.show()
