from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import explode
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, send_file, render_template, request
from matplotlib.figure import Figure
import pandas as pd
import seaborn as sns

app = Flask(__name__)
@app.route("/Query/<qid>", methods=['GET'])
def hello(qid):
 id = int(qid)
 if(id == 1):
     query1 = spark.sql("SELECT place.country as country,count(*) AS count FROM tweetDatatable where place.country is not null GROUP BY place.country ORDER BY count DESC limit 10")
     pd1 = query1.toPandas()
     print(pd1)
     img1 = BytesIO()
     pd1.plot(kind="area", x="country", y="count", title="Tweets from different countries",figsize=(10,5))
     #pd1.plot.pie(y='NumberOfTweets',labels=['IPhone', 'Web','Android','IPad'],autopct='%.2f',figsize=(5,5),title="Source of Tweets")
     plt.savefig(img1)
     img1.seek(0)
     return send_file(img1, mimetype="image/png")

 if(id ==2):
     query2 = spark.sql(
         "select count(*) as NumberOfTweets, 'Android' as Source from tweetDatatable where source like '%Twitter for Android%' UNION select count(*) as NumberOfTweets, 'IPhone' as Source from tweetDatatable where source like '%Twitter for iPhone%' UNION select count(*) as NumberOfTweets, 'IPad' as Source from tweetDatatable where source like '%Twitter for iPad%' UNION select count(*) as NumberOfTweets, 'Web' as Source from tweetDatatable where source like '%Twitter Web App%'")
     pd2 = query2.toPandas()
     img1 = BytesIO()
     pd2.plot.pie(y='NumberOfTweets', labels=['IPhone', 'Web', 'Android', 'IPad'], autopct='%.2f', figsize=(5, 5),
                  title="Source of Tweets")
     plt.savefig(img1)
     img1.seek(0)
     return send_file(img1, mimetype="image/png")

 if(id == 3):
     query3 = spark.sql("select count(*) as count,lang as language from tweetDatatable where lang is not null group by lang order by count desc limit 5")
     print(query3)
     pd3 = query3.toPandas()
     pd3.plot(kind="bar", x="language", y="count", title="top 5 languages on web series twitter data")
     img3 = BytesIO()
     plt.savefig(img3)
     img3.seek(0)
     return send_file(img3, mimetype="image/png")

 if(id == 4):
     query4 = spark.sql(
         "select user.name as user_name,user.followers_count as followers_count from tweetDatatable order by user.followers_count desc limit 10").dropDuplicates()
     pd4 = query4.toPandas()
     # pd.plot.area(x = "language",y="tweets",data=pd)
     pd4.plot.area(x='user_name',y='followers_count',title="Users with maximum number of followers")
     img4 = BytesIO()
     plt.savefig(img4)
     img4.seek(0)
     return send_file(img4, mimetype="image/png")

 if(id == 5):
     query5 = spark.sql(
         "SELECT user.name as name, max(user.favourites_count) as favourites_count FROM tweetDatatable WHERE text like '%series%' group by user.name order by favourites_count desc limit 5")
     pd5 = query5.toPandas()
     pd5.plot.area(x="name", y="favourites_count", title="Users with maximum number of favourites count")
     img5 = BytesIO()
     plt.savefig(img5)
     img5.seek(0)
     return send_file(img5, mimetype="image/png")


 if(id == 6):
     query6 = spark.sql(
         "select count(*) as count,q.text as text from (select case when text like '%prison break%' then 'prison break' when text like '%black mirror%' then 'black mirror' when text like '%crown%' then 'crown' when text like '%arrow%' then 'arrow' WHEN text like '%flash%' THEN 'flash' WHEN text like '%Game of Thrones%' THEN 'Game of Thrones' WHEN text like '%breaking bad%' THEN 'breaking bad' else 'different series' end as text from tweetDatatable)q group by q.text order by count desc limit 5")
     pd6 = query6.toPandas()
     print(pd6)
     # sns.catplot(x='candidate', y='polarity',data=pd6)
     # plt.title("")
     # pd6.plot(kind='line', x='candidate', y='polarity', color='red', ax=ax)
     sns.catplot(x='text', y='count', data=pd6).set(title="Number of Tweets on desired series")
     img6 = BytesIO()
     plt.savefig(img6)
     img6.seek(0)
     return send_file(img6, mimetype="image/png")


 # if(id == 7):
 #     query7 = spark.sql(
 #         "SELECT user.friends_count as followers_count,q.text as text from (select case when text like '%prison break%' then 'prison break' when text like '%black mirror%' then 'black mirror' when text like '%crown%' then 'crown' when text like '%arrow%' then 'arrow' WHEN text like '%flash%' THEN 'flash' WHEN text like '%Game of Thrones%' THEN 'Game of Thrones' WHEN text like '%breaking bad%' THEN 'breaking bad' else 'different series' end as text from tweetDatatable)q group by q.text order by followers_count desc limit 5")
 #     pd7 = query7.toPandas()
 #     # pd.plot(kind='bar',x='user_name',y="retweet_count")
 #     pd7.plot.pie(y="followers_count", labels=pd7.screen_name.tolist(),autopct='%.2f',
 #                 title='Followers count on desired series')
 #
 #     img7 = BytesIO()
 #     plt.savefig(img7)
 #     img7.seek(0)
 #     return send_file(img7, mimetype="image/png")

 if(id == 8):
     query8 = spark.sql(
         "SELECT user.screen_name as screen_name,text,retweeted_status.retweet_count as retweet_count FROM tweetDatatable ORDER BY retweeted_status.retweet_count DESC LIMIT 20")
     # query8.createOrReplaceTempView("df1")
     # subQuery8 =spark.sql(
     #     "SELECT name,followers_count FROM (SELECT *, MAX(followers_count) OVER (PARTITION BY name) AS maxB FROM df1) M WHERE followers_count = maxB")
     pd8 = query8.toPandas()
     pd8.plot(x="screen_name", y="retweet_count",title='Users with highest number of retweets',figsize=(10,5))
     img8 = BytesIO()
     plt.savefig(img8)
     img8.seek(0)
     return send_file(img8, mimetype="image/png")


 if(id == 9):
     query9 = spark.sql(
         "SELECT user.location as location,count(text) as count FROM tweetDatatable WHERE place.country='United States' AND user.location is not null GROUP BY user.location ORDER BY count DESC LIMIT 5")
     pd9 = query9.toPandas()
     #pd9.plot.pie(y='count',labels=pd9.location,autopct='%.2f',figsize=(5,5),title="Source of Tweets")
     sns.catplot(x="location", y="count",kind="point",data=pd9,title='Top 5 places with highest number of tweets')
     img9 = BytesIO()
     plt.savefig(img9)
     img9.seek(0)
     return send_file(img9, mimetype="image/png")

 if(id == 10):
     query10 = spark.sql(
         "select count(*) as tweets, 'Hulu' as Source from tweetDatatable where text like '%hulu%' or text like'%Hulu%' UNION select count(*) as tweets, 'Netflix' as Source from tweetDatatable where text like '%netflix%' or text like '%Netflix%' UNION select count(*) as tweets, 'Amazon Prime' as Source from tweetDatatable where text like '%prime%' or text like '%amazonprime%'")
     pd10 = query10.toPandas()
     pd10.plot(linestyle='-.',title="Number of tweets on prime,netflix and hulu",x='Source',y="tweets",figsize=(8,5))
     # pd3.plot(kind="bar", x="Party", y="tweets", title="Republicans and Democratic tweets with 100% positivity")
     img10 = BytesIO()
     plt.savefig(img10)
     img10.seek(0)
     return send_file(img10, mimetype="image/png")


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()
    # spark is an existing SparkSession
    df = spark.read.json(r"C:\Users\jagad\Downloads\teams.json")
    df.createOrReplaceTempView("tweetDatatable")
    app.run(debug=True)