from flask import request, url_for, jsonify
from flask_api import FlaskAPI, status, exceptions
from pymongo import MongoClient

app = FlaskAPI(__name__)

@app.route("/", methods=['GET'])
def list():
    mongo_uri = "mongodb://mongo-router:27017"

    client = MongoClient(mongo_uri)

    #Database
    db = client.MusicStore

    #Collections
    productsCollection = db.products
    salesCollection = db.sales
    clientsCollection = db.clients

    #First querie (in products collection)
    #All items sorted by price and only the first thousand
    pipeline = [ {"$project": {"_id":"$_id","cd_name":"$cd_name","price":"$price"}}, {"$sort": {"price":-1}}, {"$limit": 1000} ]
    cursor = productsCollection.aggregate(pipeline)
    return cursor

    #Second search (in clients collection)
    #All id's and names of the female clients, sorted by their id's
    pipeline = [{"$match": {"gender": "female"}}, {"$group": {"_id":"$_id", "name": {"$first": "$name"}}}, {"$sort": {"_id":1}} ]
    cursor = clientsCollection.aggregate(pipeline)
    return cursor

    #third search (in sales collection)
    #The average of the products bought by all the clients
    pipeline = [ {"$group": {"_id":"average", "avg_discs_bought": {"$avg": {"$size": "$productos"}}}} ]
    cursor = salesCollection.aggregate(pipeline)
    return cursor

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
