# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/2023_matchday_results-1.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
pl_results_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)



pl_results_df=pl_results_df.withColumnRenamed("Sl.no","sl_no") \
    .withColumnRenamed("fixture.id", "fixture_id") \
    .withColumnRenamed("fixture.date","fixture_date") \
    .withColumnRenamed("teams.home.id","teams_home_id") \
    .withColumnRenamed("teams.home.winner","teams_home_winner") \
    .withColumnRenamed("teams.home.name","teams_home_name") \
    .withColumnRenamed("teams.away.id","teams_away_id") \
    .withColumnRenamed("teams.away.name","teams_away_name") \
    .withColumnRenamed("teams.away.winner","teams_away_winner") \
    .withColumnRenamed("goals.home","goals_home") \
    .withColumnRenamed("goals.away","goals_away")
# Create a view or table

temp_table_name = "pl_results"
#display(pl_results_df)
pl_results_df.printSchema()
pl_results_df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/2023_home_teams_stats.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
pl_home_team_stats_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

pl_home_team_stats_df=pl_home_team_stats_df.withColumnRenamed("Home team id","home_team_id") \
    .withColumnRenamed("fixture id", "fixture_id") \
    .withColumnRenamed("Home team name","Home_team_name") \
    .withColumnRenamed("Shots on Goal","Shots_on_Goal") \
    .withColumnRenamed("Shots off Goal","Shots_off_Goal") \
    .withColumnRenamed("Total Shots","Total_Shots") \
    .withColumnRenamed("Blocked Shots","Blocked_Shots") \
    .withColumnRenamed("Shots insidebox","Shots_insidebox") \
    .withColumnRenamed("Shots outsidebox","Shots_outsidebox") \
    .withColumnRenamed("Corner Kicks", "Corner_Kicks") \
    .withColumnRenamed("Ball Possession", "Ball_Possession") \
    .withColumnRenamed("Yellow Cards","Yellow_Cards") \
    .withColumnRenamed("Red Cards","Red_Cards") \
    .withColumnRenamed("Goalkeeper Saves","Goalkeeper_Saves") \
    .withColumnRenamed("Total passes","Total_passes") \
    .withColumnRenamed("Passes accurate","Passes_accurate") \
    .withColumnRenamed("Passes %","Passes_%")

#display(pl_home_team_stats_df)
pl_home_team_stats_df.printSchema()
temp_table_name = "home_teams_stats"

pl_home_team_stats_df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/2023_away_teams_stats.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
pl_away_teams_stats_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

pl_away_teams_stats_df=pl_away_teams_stats_df.withColumnRenamed("Away team id","away_team_id") \
    .withColumnRenamed("fixture id", "fixture_id") \
    .withColumnRenamed("Away team name","Away_team_name") \
    .withColumnRenamed("Shots on Goal","Shots_on_Goal") \
    .withColumnRenamed("Shots off Goal","Shots_off_Goal") \
    .withColumnRenamed("Total Shots","Total_Shots") \
    .withColumnRenamed("Blocked Shots","Blocked_Shots") \
    .withColumnRenamed("Shots insidebox","Shots_insidebox") \
    .withColumnRenamed("Shots outsidebox","Shots_outsidebox") \
    .withColumnRenamed("Corner Kicks", "Corner_Kicks") \
    .withColumnRenamed("Ball Possession", "Ball_Possession") \
    .withColumnRenamed("Yellow Cards","Yellow_Cards") \
    .withColumnRenamed("Red Cards","Red_Cards") \
    .withColumnRenamed("Goalkeeper Saves","Goalkeeper_Saves") \
    .withColumnRenamed("Total passes","Total_passes") \
    .withColumnRenamed("Passes accurate","Passes_accurate") \
    .withColumnRenamed("Passes %","Passes_%")

#display(pl_away_teams_stats_df)
pl_away_teams_stats_df.printSchema()

temp_table_name = "away_teams_stats"

pl_away_teams_stats_df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table pl_results;
# MAGIC describe table home_teams_stats;
# MAGIC describe table away_teams_stats;

# COMMAND ----------

#1
pl_results_df.select("fixture_id","fixture_date","teams_home_name","teams_away_name","goals_home","goals_away")\
.where("teams_home_name == 'Manchester City' and teams_home_winner == true")\
.count()

# COMMAND ----------

#2
pl_results_df.select("fixture_id","fixture_date","teams_home_name","teams_away_name","goals_home","goals_away")\
.where("teams_away_name == 'Manchester City' and teams_home_winner is null")\
.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #3 Team with most home wins in entire season
# MAGIC with team_home_wins_count as (select
# MAGIC teams_home_name,count(teams_home_winner) as total_home_wins from pl_results
# MAGIC where teams_home_winner = 'true'
# MAGIC group by teams_home_name)
# MAGIC select teams_home_name, total_home_wins from team_home_wins_count
# MAGIC where total_home_wins = (select max(total_home_wins) from team_home_wins_count)

# COMMAND ----------

most_home_wins_df = _sqldf
most_home_wins_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #4 Team with most home losses in entire season
# MAGIC with team_home_wins_count as (select
# MAGIC teams_home_name,count(teams_home_winner) as total_home_losses from pl_results
# MAGIC where teams_home_winner = 'false'
# MAGIC group by teams_home_name)
# MAGIC select teams_home_name, total_home_losses from team_home_wins_count
# MAGIC where total_home_losses = (select max(total_home_losses) from team_home_wins_count)

# COMMAND ----------

most_home_losses_df = _sqldf
most_home_losses_df.show()

# COMMAND ----------

#5
pl_results_df.select("fixture_id","fixture_date","teams_home_name")\
.where("teams_home_name == 'Manchester City' and teams_home_winner == true")\
.show(10)


# COMMAND ----------

#6 Games that ended in a draw
pl_results_df.filter((pl_results_df.teams_home_winner.isNull())& (pl_results_df.teams_away_winner.isNull())).count()

# COMMAND ----------

#7 Games where more than 5 goals were scored by home team and 0 by away team
pl_results_df.filter((pl_results_df.goals_home > 5 )& (pl_results_df.goals_away == 0)).show()

# COMMAND ----------

pl_home_team_stats_df.select("home_team_id","home_team_name").show(5)

# COMMAND ----------

#for filter based functions check this link : sparkbyexamples

# COMMAND ----------

#8
pl_results_df.join(pl_home_team_stats_df, pl_home_team_stats_df.fixture_id == pl_results_df.fixture_id, "inner").where("teams_home_name='Chelsea' and teams_away_name='Manchester City'").select("teams_home_name","teams_away_name","Shots_On_Goal").show(2)

# COMMAND ----------

#dbutils.fs.help()
#dbutils.notebook.help()
#dbutils.widgets.help()
#dbutils.secrets.help()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")
#dbutils.fs.head("/FileStore/tables/2023_home_teams_stats.csv")
#COPY, MKDIRS, MV, PUT, RM

# COMMAND ----------

#9 Storing in Parquet format
pl_results_df.write.parquet("/FileStore/tables/2023_matchday_results-1.parquet")

# COMMAND ----------

# Retrieving parquet format
p_df = spark.read.parquet("/FileStore/tables/2023_matchday_results-1.parquet")

# COMMAND ----------

#p_df.count()
pl_results_df.printSchema()

# COMMAND ----------

#10
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("JSON Creation").getOrCreate()

# Define the PySpark DataFrame schema
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("city", StringType())
])

# Create a PySpark DataFrame
data = [("Shyam", 25, "New York"),
        ("Ram", 30, "San Francisco")]
df = spark.createDataFrame(data, schema)

# Convert the PySpark DataFrame to a JSON string
json_string = df.toJSON().collect()

pl_result_json = pl_results_df.toJSON().collect()[0]

print(pl_result_json)

# COMMAND ----------

#11 Total goals scored by each teams when played at home stadium
from pyspark.sql.functions import *
from pyspark.sql.types import *
#pl_results_df.groupBy("teams_home_name").agg(avg("goals_home")).orderBy("teams_home_name").collect()
pl_results_df.groupBy("teams_home_name").agg((sum("goals_home")+sum("goals_away")).alias("total Goal Scored")).orderBy("teams_home_name").show()


# COMMAND ----------

#12
pl_results_df.alias("pl_results").join(
pl_home_team_stats_df.alias("home_team_stats"),col("pl_results.fixture_id") == col("home_team_stats.fixture_id"),
"inner").select("home_team_stats.fixture_id","pl_results.teams_home_name","pl_results.teams_away_name").show(5)

# COMMAND ----------

#13 Total shots taken by each teams in entire season when playing at home
pl_results_df.alias("pl_results") \
.join(pl_home_team_stats_df.alias("home_team_stats") \
      ,col("pl_results.fixture_id") == col("home_team_stats.fixture_id"), \
      "inner") \
      .groupBy("pl_results.teams_home_name") \
      .agg(sum(pl_home_team_stats_df.Total_Shots).alias("total_shots")) \
      .show()

# COMMAND ----------

#14
pl_results_df.alias("pl_results").join(
pl_home_team_stats_df.alias("home_team_stats"),col("pl_results.fixture_id") == col("home_team_stats.fixture_id"),
"inner").select("home_team_stats.fixture_id","pl_results.teams_home_name","pl_results.teams_away_name").show(5)

# COMMAND ----------

#15 Total fould committed by each team in the entire season both home and away
from pyspark.sql.functions import *
from pyspark.sql.types import *
result = pl_results_df.alias("pl_results") \
.join(pl_home_team_stats_df.alias("home_team_stats") \
      ,col("pl_results.fixture_id") == col("home_team_stats.fixture_id") \
      ,"inner") \
        .join(pl_away_teams_stats_df.alias("away_team_stats") \
        ,col("away_team_stats.fixture_id")==col("pl_results.fixture_id") \
        ,"inner") \
      .groupBy("pl_results.teams_home_name") \
     .agg((sum("home_team_stats.Fouls")+sum("away_team_stats.Fouls")).alias("Total_Fouls")) \
      .orderBy("pl_results.teams_home_name").show()
    #.withColumnRenamed("sum(home_team_stats.Fouls)+sum(away_team_stats.Fouls)","Total_Fouls").show()
#result.printSchema()
#result.select(col("sum(home_team_stats.Fouls) + sum(away_team_stats.Fouls)")).show()

# COMMAND ----------

#16 average goals scored by team playing at home, threshold be 1
threshold=1
result = pl_results_df.groupBy("teams_home_name").agg(avg("goals_home").alias("avg_goals")).filter(col("avg_goals") > threshold).orderBy(col("avg_goals").desc())
result.show()