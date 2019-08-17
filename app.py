from flask import Flask
import os

app = Flask(__name__)
port = os.environ.get('PORT')

@app.route('/')
def index():
   return '<html><body><h1>Hello World</h1></body></html>'

app.run(port=port, host="0.0.0.0")