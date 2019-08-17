from flask import Flask, jsonify
import os

app = Flask(__name__)
port = os.environ.get('PORT')

@app.route('/')
def index():
   return jsonify( 
    status=200, 
    replies=[{ 
      'type': 'text', 
      'content': 'Hello World'
    }]
   )

app.run(port=port, host="0.0.0.0")
