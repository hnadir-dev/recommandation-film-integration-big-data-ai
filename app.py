import logging
from elasticsearch import Elasticsearch

from flask import Flask, render_template, request
import pandas as pd
import json

from get_movie_image import MovieInfo

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

# client = Elasticsearch(
#         ['https://hn-search-9858436033.us-east-1.bonsaisearch.net:443'],
#         http_auth=("t2mABiEPJn","KwyXmSUap5AMfcR34Lvi"))
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

def get_es_records_df():
    index_name = 'rec-movies-index'

    query = {
        "query": {
            "match_all": {}
        }
    }

    result = es.search(index=index_name, body=query, size=2000)
    redata = map(lambda x:x['_source'], result['hits']['hits'])


    return pd.DataFrame(redata)

@app.route("/")
@app.route("/index")
def index():
    # Successful response!
    suggestions = get_all_movies_names()

    return render_template('index.html',suggestions=suggestions)


@app.route('/recommandation', methods=['GET', 'POST'])
def main():
    if request.method == 'GET':
        return(render_template('index.html'))
    
    if request.method == 'POST':
        m_name = request.form['movie_name']
        # m_name = m_name.title()
    
    # if '(' in m_name:
    #     m_name = m_name.split('(')[0]
    
    m_image = MovieInfo.get_movie_poster_url(m_name.split('(')[0])
    
    return render_template('recommandation.html',movie_name=m_name.split('(')[0], movie_image=m_image,movie_year=m_name.split('(')[1].split(')')[0],movie_info=get_movie_from_name(m_name))

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
