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
file_location = "/FileStore/tables/ratings.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

dbutils.fs.ls ("/FileStore/tables")

# COMMAND ----------

# Create a view or table

temp_table_name = "ratings_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `ratings_csv`

# COMMAND ----------

#Read links.csv file
 
df = spark.read.format("csv").option("header",True).load("/FileStore/tables/links.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

import datetime
import pyspark.sql.functions as f
import pyspark.sql.types 
import pandas as pd
 
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql import window
from pyspark.sql.functions import rank,min

# COMMAND ----------

#File location and type
 
file_location = "/FileStore/tables/movies.csv"
 
file_type = "csv"
 
#csv options
 
infer_schema = "true"
first_row_is_header = "true"
delimiter = ','
 
 
df_movies = spark.read.format(file_type)\
          .option("inferSchema",infer_schema)\
          .option("header",first_row_is_header)\
          .option("sep",delimiter)\
          .load(file_location)
 
display(df_movies)

# COMMAND ----------

#create a View or Table
 
temp_table_name = "movies_csv"
df_movies.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC select * from movies_csv;

# COMMAND ----------

#Read links file
 
 
df_links = spark.read.format(file_type)\
                .option("inferSchema",infer_schema)\
                .option("header",first_row_is_header)\
                .option("sep",delimiter)\
                .load("/FileStore/tables/links.csv")
display(df_links)

# COMMAND ----------

#Read tags file
 
df_tags = spark.read.format(file_type)\
                .option("inferSchema",infer_schema)\
                .option("header",first_row_is_header)\
                .option("sep",delimiter)\
                .load("/FileStore/tables/tags.csv")
display(df_tags)

# COMMAND ----------

#Read ratings file
 
df_ratings = spark.read.format(file_type)\
                   .option("inferSchema",infer_schema)\
                   .option("header",first_row_is_header)\
                   .option("sep",delimiter)\
                   .load("/FileStore/tables/ratings.csv")
                  
display(df_ratings)

# COMMAND ----------

#count of records
 
df_movies.count()
display(df_movies)

# COMMAND ----------

df_links.count()
display(df_links)

# COMMAND ----------

df_tags.count()
display(df_tags)

# COMMAND ----------

#Movies with ratings: joining moveis DF with ratings DF
 
df_movies_with_ratings =  df_movies.join(df_ratings,"movieid","left")
display(df_movies_with_ratings)

# COMMAND ----------

df_movies_with_ratings.count()

# COMMAND ----------

#Check if there are duplicates (Movies with more than 1 rating) ---> more the count more the rating
 
df_movies_no_dups = df_movies_with_ratings.groupby("movieid").count()
display(df_movies_no_dups)

# COMMAND ----------

#Join our ratings DF with tags DF (Join with users dataset)
 
df_ratings_tags = df_ratings.join(df_tags,"movieid","inner")
display(df_ratings_tags)

# COMMAND ----------

df_ratings.describe()

# COMMAND ----------

#Adding new column from timestamp col using from_unixtime: Date conversion
 
df_ratings = df_ratings.withColumn("tsDate", f.from_unixtime("timestamp"))

# COMMAND ----------

df_ratings.describe()

# COMMAND ----------

display(df_ratings)

# COMMAND ----------

#String to Date conversion in the required format
df_ratings = df_ratings.select('userId','movieId','rating', f.to_date(unix_timestamp('tsDate','yyyy-MM-dd HH:MM:SS').cast("timestamp")).alias("ratingDate"))
 

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
display(df_ratings)

# COMMAND ----------

#year wise rating count
df_ratings_year = df_ratings.groupby("ratingDate").count()
display(df_ratings_year)

# COMMAND ----------

#finding Average ratings
 
df_avg_ratings = df_ratings.groupby("movieId").mean("rating")
display(df_avg_ratings)

# COMMAND ----------

#Join avg ratings DF with movies Df on movie_ID
 
df = df_avg_ratings.join(df_movies,"movieId","inner")
df = df.withColumnRenamed("avg(rating)","average_rating")
display(df)

# COMMAND ----------

#What do we have now in the DF?
 
df.printSchema()

# COMMAND ----------

#Grouping by rating count! -> Which movie have been rated how many times?
 
df_total_ratings = df_ratings.groupby("movieId").count()
display(df_total_ratings)

# COMMAND ----------

#Filtering movies which are having less than 5 ratings in total!
 
df_total_ratings = df_total_ratings.where("count > 10")
df_ratings_filtered = df_ratings.join(df_total_ratings,"movieId",'inner')

# COMMAND ----------

display(df_ratings_filtered)

# COMMAND ----------

#how many records do we have after filtering out the no. of ratings > 10
 
df_total_ratings.count()

# COMMAND ----------

#max rating per user
 
df_rating_per_user = df_ratings_filtered.select('userId','movieId','rating').groupby('userId','movieId').max('rating')
display(df_rating_per_user)

# COMMAND ----------

#joining df_rating_per user with out movies DF
 
df_rating_per_user_movie = df_rating_per_user.join(df_movies,'movieId','inner')

# COMMAND ----------

#renaming max column into some good name
 
df_rating_per_user_movie = df_rating_per_user_movie.withColumnRenamed("max(rating)","Max_rating")
display(df_rating_per_user_movie)

# COMMAND ----------

temp_table_movie_with_ratings = "tb_rating_per_user_movie"
df_rating_per_user_movie.createOrReplaceTempView(temp_table_movie_with_ratings)

# COMMAND ----------

df_10 = spark.sql("""select * from tb_rating_per_user_movie where title Like 'Fugitive%'""")
display(df_10)

# COMMAND ----------

df_query = spark.sql("""select * from tb_rating_per_user_movie""")
display(df_query)

# COMMAND ----------

df_rating = df_rating_per_user_movie.groupby('movieId','userId','title','genres').max('Max_rating')
display(df_rating)

# COMMAND ----------

#users with movies > 4 ratings
 
df_rating = df_rating.withColumnRenamed('max(Max_rating)', 'max_rating') 
df_rating = df_rating.where('max_rating >= 4') 
display(df_rating)

# COMMAND ----------

#Best Movie per genre
df_movies_genre = df_rating.groupby('genres','title').count()
display(df_movies_genre)

# COMMAND ----------

#identity genre of user
 
df_rating_genre = df_rating.select('userId','title','genres').groupby('userId','genres').count()
display(df_rating_genre)

# COMMAND ----------

#DF recent movie
 
df_recent_movie = df_ratings.groupby('userId','movieId').agg(f.max(df_ratings['ratingDate']))
display(df_recent_movie)

# COMMAND ----------


#Visualization 1
 
display(df_movies_genre)
#movies_df['genres_arr'] = movies_df['genres'].str.split('|')
#movies_df.head()
#counter_lambda = lambda x: len(x)
#movies_df['genre_count'] = movies_df.genres_arr.apply(counter_lambda)
#movies_df.head()
 
#movies_df.set_index('movieId')
#movies_df.head()

# COMMAND ----------

df_movies_genre_viz = df_rating.toPandas()
#df_movies_genre['genre_arr'] = df_movies_genre['genres'].str.split('|')
#df_movies_genre.head()

# COMMAND ----------

df_movies_genre_viz['genre_arr'] = df_movies_genre_viz['genres'].str.split('|')

# COMMAND ----------


df_movies_genre_viz.head()

# COMMAND ----------

counter_lambda = lambda x: len(x) #counter gets incremented each time
df_movies_genre_viz['genre_count'] = df_movies_genre_viz.genre_arr.apply(counter_lambda)
df_movies_genre_viz.head()
 
df_movies_genre_viz.set_index('movieId')
df_movies_genre_viz.head()

# COMMAND ----------

from collections import Counter
 
import pandas as pd 
from pandas import DataFrame as df
import numpy as np 
import matplotlib.pyplot as plt
%matplotlib inline
 
flattened_genres = [item for sublist in df_movies_genre_viz.genre_arr for item in sublist]
 
genre_dict = dict(Counter(flattened_genres))
 
print(genre_dict)
 
# now lets plot this genre distribution as a pie chart
plt.pie(genre_dict.values(), labels=genre_dict.keys())
plt.title('Genre distribution of movies')
plt.show()
 
#plt.savefig('./movie-genres-pie.png')

# COMMAND ----------

import matplotlib.pyplot as plt
from collections import Counter

# Number of genres against number of movies
plt.hist(df_movies_genre_viz.genre_count, edgecolor='black', linewidth=1.2)
plt.title("Genres Histogram")
plt.xlabel("# of genres")
plt.ylabel("# of movies")
plt.axis([0, 9, 0, 5000])
plt.show()

# List of genres
flattened_genres = [item for sublist in df_movies_genre_viz.genres for item in sublist]
genre_dict = dict(Counter(flattened_genres))
print(genre_dict)

# COMMAND ----------

# For better readability
x = list(range(len(genre_dict)))
plt.title('No. of movies vs No. of genres')
plt.xticks(x, genre_dict.keys(), rotation=80)
plt.bar(x, genre_dict.values())
plt.xlabel("Number of genres")
plt.ylabel("Number of movies")
plt.grid()
plt.plot()

# COMMAND ----------

df_Pandas_ratings = df_ratings.toPandas()

# COMMAND ----------

df_Pandas_ratings.head()

# COMMAND ----------

# Histogram of movie ratings
plt.hist(df_Pandas_ratings.rating,rwidth=0.5)
plt.xticks([0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0])
plt.xlabel('Rating')
plt.ylabel('# of movies')
plt.grid()
plt.show()

# COMMAND ----------

# Top 25 movies
 
#most_rated=movielens.groupby('title').size().sort_values(ascending=False)[:25]
 
movie_lens = df_rating.toPandas

# COMMAND ----------

#most_rated = movie_lens.groupby('title').size().sort_values(ascending=False)[:25]
display(df_rating)

# COMMAND ----------

df_genres_count = df_rating.groupby('genres').count()

# COMMAND ----------

display(df_genres_count)

# COMMAND ----------


