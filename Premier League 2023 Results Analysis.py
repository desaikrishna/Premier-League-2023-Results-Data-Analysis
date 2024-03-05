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