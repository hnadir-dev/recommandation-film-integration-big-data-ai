import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())

from pyspark.sql.functions import avg, desc, col

from pyspark.ml.recommendation import ALS, ALSModel

import logging
from elasticsearch import Elasticsearch

from flask import Flask, render_template, request, jsonify
import pandas as pd
import json

from get_movie_image import MovieInfo

def create_spark_configuration():
    spark_config = None
    try:
        spark_config = (SparkSession.builder
            .appName("ElasticsearchSparkIntegration")
            # .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-20_2.12:7.17.14,"
            #         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")
            .getOrCreate())
        
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_config

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)


es_host = 'http://127.0.0.1:9200'
es = Elasticsearch(hosts=[es_host])

#for suggestions
def get_all_movies_names():
    
    index_name = 'rec-movies-index'

    query = {
            "_source": ["name"], 
            "query": {
                "match_all": {}
            }
        }

    result = es.search(index=index_name, body=query, size=2000)
    redata = map(lambda x:x['_source'], result['hits']['hits'])

    titles = pd.DataFrame(redata)['name']

    return json.loads(titles.to_json(orient='records'))

def get_movie_from_name(name):
    index_name = 'rec-movies-index'

    query = {
            "query": {
                "match": {
                    "name": name
                }
            }
        }
    result = es.search(index=index_name, body=query, size=2000)
    redata = map(lambda x:x['_source'], result['hits']['hits'])

    return json.loads(pd.DataFrame(redata).to_json(orient='records'))[0]

def get_users_from_movieId(movieId):
    index_name = 'rec-ratings-index'

    query = {
        "_source": ["userId"], 
        "query": {
            "match": {
            "movieId": movieId
            }
        }
    }

    result = es.search(index=index_name, body=query, size=2000)
    redata = map(lambda x:x['_source'], result['hits']['hits'])

    return json.loads(pd.DataFrame(redata).to_json(orient='records'))

def get_recommanded_movies(ids):
    index_name = 'rec-movies-index'

    query = {
        "query":{
            "terms": {
            "movieId": ids
            }
        }
    }

    result = es.search(index=index_name, body=query, size=2000)
    redata = map(lambda x:x['_source'], result['hits']['hits'])

    movies = pd.DataFrame(redata)
    #change url image beacause the first one not working
    movies['url'] = movies['name'].apply(lambda x:MovieInfo.get_movie_poster_url(x.split('(')[0]))

    return json.loads(movies.to_json(orient='records'))

@app.route("/")
@app.route("/index")
def index():
    # Successful response!
    suggestions = get_all_movies_names()

    return render_template('index.html',suggestions=suggestions)

spark = create_spark_configuration()
model = ALSModel.load('model/als-rec')
@app.route('/recommandation', methods=['GET', 'POST'])
def main():
    # spark = create_spark_configuration()
    # model = ALSModel.load('model/als-rec')
    
    if request.method == 'GET':
        return(render_template('index.html'))
    
    if request.method == 'POST':
        m_name = request.form['movie_name']
        # m_name = m_name.title()

    current_movie_info = get_movie_from_name(m_name)
    
    all_users = get_users_from_movieId(current_movie_info['movieId'])
    
    #function to get movie image from api MovieLen
    m_image = MovieInfo.get_movie_poster_url(m_name.split('(')[0])

    df_all_users = spark.createDataFrame(all_users)
    pr =  model.recommendForUserSubset(df_all_users,10)
    
    all_movies_recommandaded = []
    for rec in pr.select('recommendations').collect():
        for item in rec['recommendations']:
            movies_recommandaded = {}
            movies_recommandaded['movieId'] = item.movieId
            movies_recommandaded['rating'] = item.rating
            all_movies_recommandaded.append(movies_recommandaded)

    #print(all_movies_recommandaded)
    p = spark.createDataFrame(all_movies_recommandaded)
    final_rec_movies = p.groupBy('movieId').agg(avg('rating').alias('avg')).filter(col('avg') > 7).sort(desc('avg'))#.toPandas().to_json(orient='records')


    final_rec_movies_list = [mid['movieId'] for mid in json.loads(final_rec_movies.select('movieId').toPandas().to_json(orient='records'))]
    

    return render_template('recommandation.html',
                           movie_name=m_name.split('(')[0], 
                           movie_image=m_image,movie_year=m_name.split('(')[1].split(')')[0],
                           movie_info=current_movie_info,
                           final_rec_movies=get_recommanded_movies(final_rec_movies_list))

#--------+--------+--------+--------+--------+--------#
#                          APIs                       #
#--------+--------+--------+--------+--------+--------#
@app.route("/movies/<int:index>")
def movies(index):
    jsf = open('data/json/movies.json','r+')
    data = json.load(jsf)

    movies = data['results']

    if index > data['count']:
        return dict({'message':'Entre a valide index!','error_id': 404})
    else:
        return movies[index]

@app.route("/users/<int:index>")
def users(index):
    jsf = open('data/json/users.json','r+')
    data = json.load(jsf)

    users = data['results']

    if index > data['count']:    
        return dict({'message':'Entre a valide index!','error_id': 404})
    else:
        return users[index]

@app.route("/ratings/<int:index>")
def ratings(index):
    jsf = open('data/json/ratings.json','r+')
    data = json.load(jsf)

    ratings = data['results']

    if index > data['count']:    
        return dict({'message':'Entre a valide index!','error_id': 404})
    else:
        return ratings[index]


if __name__ == '__main__':
    app.run(debug=True)
