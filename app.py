from flask import Flask,render_template
from asgiref.wsgi import WsgiToAsgi


app = Flask(__name__)


@app.route("/")
def index():
    return 'Welcome to movies recommandation'


asgi_app = WsgiToAsgi(app)