from flask import Flask, request
import json
from flask_cors import CORS, cross_origin
import Investment as mi
import Portfolio as cp

app = Flask(__name__)
CORS(app)

@app.route('/')
def hello_world():
    jsonResp = mi.initialInvestment(request.args.get('budget'))
    print(json.dumps(jsonResp))
    return json.dumps(jsonResp)

@app.route('/buysell')
def hello_world2():
    jsonResp = cp.buyandsell()
    print(json.dumps(jsonResp))
    return json.dumps(jsonResp)

if __name__== '__main__':
    app.run()