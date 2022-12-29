from flask import Flask
from flask_cors import CORS
app = Flask(__name__)
CORS(app)
from pyspark.sql import SparkSession
from collections import Counter
from flask import json
from flask import jsonify
from pyspark import SparkContext
import pyspark
from pyspark.sql import SQLContext  # run sql queries using pyspqrk
spark = SparkSession.builder.appName("myApp") \
.config("spark.mongodb.input.uri",
"mongodb://127.0.0.1/bigdata.items") \
.config("spark.mongodb.output.uri",
"mongodb://127.0.0.1/bigdata.items") \
.config('spark.jars.packages',
'org.mongodb.spark:mongo-spark-connector_2.12:2.4.2') \
.getOrCreate()


sqlC = SQLContext(spark)


# python -m flask run
 
@app.route('/api/topjournals/')
def topjournals():
    df = spark.read.format("mongo").load()
    pandas_df = df.groupBy("Publisher").count().sort("count",ascending=True).toPandas().tail(7)
    return pandas_df.to_json(orient='records').replace('count','value').replace('journal','name')
@app.route('/api/collaborationc/')
def getCountriesCol():
    df = spark.read.format("mongo").load()
    pandas_df = df.groupBy("countries").count().sort(
    "count", ascending=True).toPandas()
    pandas_df = pandas_df[pandas_df['countries'].str.contains(";")]
    vocab = Counter()
    for index, row in pandas_df.iterrows():
        a = row['countries'].strip().replace('; ', ';')
        sorted_words = ';'.join(a.lower().split(";"))
        vocab[sorted_words] += int(row['count'])
    result = [{'name': key, 'value': value} for key, value in vocab.items()]
    return jsonify(result)


@app.route('/api/year/')
def meteo():
    df = spark.read.format("mongo").load()
    pandas_df = df.groupBy("Year").count().sort("count", ascending=True).toPandas()
    return pandas_df.to_json(orient='records').replace('count','value').replace('Year', 'name')
@app.route('/api/countries/')
def pubContr():
    df = spark.read.format("mongo").load()
    pandas_df = df.groupBy("countries").count().sort(
    "count", ascending=True).toPandas()
    pandas_df = pandas_df[~pandas_df['countries'].str.contains(";")]
    return pandas_df.to_json(orient='records').replace('count',
    'value').replace('valueries', 'name')

@app.route('/api/scopes/')
def Scopus():
    Scraped_Data = spark.read.format("mongo").load()
    Quartile_Data = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/bigdata.scopes").load()
    Quartile_Data.createOrReplaceTempView("scopes")
    Quartile_Data = sqlC.sql("SELECT Title, `SJR Best Quartile`,Year from scopes")
    Scraped_Data_join_ScrapedData_Quartile = Scraped_Data.join(Quartile_Data, (Scraped_Data["Publisher"] == Quartile_Data["Title"]) &
    ( Scraped_Data["Year"] == Quartile_Data["Year"]),"inner")
    Articles_By_Quartile = Scraped_Data_join_ScrapedData_Quartile.groupBy("SJR Best Quartile").count()
    Scraped_Data_join_Q_Filtered_By_Q1 = Scraped_Data_join_ScrapedData_Quartile.filter("`SJR Best Quartile` == 'Q1'")
    res=Scraped_Data_join_Q_Filtered_By_Q1.groupBy("countries").count().sort("count", ascending=True).toPandas()
    return res.to_json(orient='records')

   
