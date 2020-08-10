import os
import apache_beam as beam
import MyParDos
import json
import time
import datetime
from apache_beam.io.mongodbio import ReadFromMongoDB, WriteToMongoDB
from flask import Flask, flash, request, redirect, url_for, render_template, Response
from flask_pymongo import PyMongo
from bson import json_util
from elasticsearch import Elasticsearch
import pymongo

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/ededore-archive"
app.config["SECRET_KEY"] = os.urandom(20)
mongo = PyMongo(app)


@app.route('/', methods=['GET'])
def getSummary():
    count = mongo.db.data.count_documents({})
    highAuthCount = mongo.db.data.count_documents({"authenticity": "High"})
    unclearAuthCount = mongo.db.data.count_documents(
        {"authenticity": "Unclear"})
    lowAuthCount = mongo.db.data.count_documents({"authenticity": "Low"})
    flash(str(count), "Overall number of datasets")
    flash(str(highAuthCount), "High authenticity datasets")
    flash(str(unclearAuthCount), "Unclear authenticity datasets")
    flash(str(lowAuthCount), "Low authenticity datasets")
    return render_template("index.html")


def joinDict(values):
    if values:
        return [{v[0]: v[1]} for v in values]


def hasAlias(element, alias):
    return alias.lower() in element["personAlias"].lower()


def hasTitle(element, title):
    return title.lower() in element["title"].lower()


@app.route('/emotion-occurence', methods=['GET'])
def getLatestEmotionOccurences():
    if isRenewEnforced(request):
        performPipelineOpsForGlobalEmotionOccurence()
    if isFullUpdateEnforced(request):
        performPipelineOpsForGlobalEmotionOccurence()
        removeOutdatedRecords("beam_emotion_occurence")
    return Response(
        json_util.dumps(mongo.db.beam_emotion_occurence.find_one(
            {}, sort=[("timestamp", pymongo.DESCENDING)])),
        mimetype="application/json"
    )


@app.route('/emotion-occurence-absolute', methods=['GET'])
def getLatestAbsoluteEmotionOccurences():
    if isRenewEnforced(request):
        performPipelineOpsForGlobalAbsoluteEmotionOccurences()
    if isFullUpdateEnforced(request):
        performPipelineOpsForGlobalAbsoluteEmotionOccurences()
        removeOutdatedRecords("beam_emotion_overall_occurence")
    return Response(
        json_util.dumps(mongo.db.beam_emotion_overall_occurence.find_one(
            {}, sort=[("timestamp", pymongo.DESCENDING)])),
        mimetype="application/json"
    )


@app.route('/emotion-occurence-history', methods=['GET'])
def getEmotionOccurencesHistory():
    if isRenewEnforced(request):
        performPipelineOpsForGlobalEmotionOccurence()
    if isFullUpdateEnforced(request):
        performPipelineOpsForGlobalEmotionOccurence()
        removeOutdatedRecords("beam_emotion_occurence")
    return Response(
        json_util.dumps(mongo.db.beam_emotion_occurence.find()),
        mimetype="application/json"
    )


@app.route('/emotion-occurence-absolute-history', methods=['GET'])
def getEmotionAbsoluteOccurencesHistory():
    if isRenewEnforced(request):
        performPipelineOpsForGlobalAbsoluteEmotionOccurences()
    if isFullUpdateEnforced(request):
        performPipelineOpsForGlobalAbsoluteEmotionOccurences()
        removeOutdatedRecords("beam_emotion_overall_occurence")
    return Response(
        json_util.dumps(mongo.db.beam_emotion_overall_occurence.find()),
        mimetype="application/json"
    )


@app.route('/emotion-records/<alias>', methods=['GET'])
def getEmotionRecordsByAlias(alias):
    if isRenewEnforced(request):
        performPipelineOpsForAlias(alias)
    if isFullUpdateEnforced(request):
        performPipelineOpsForAlias(alias)
        removeOutdatedRecords("beam_emotion_filtered")
    return Response(
        json_util.dumps(mongo.db.beam_emotion_filtered.find({"alias": alias})),
        mimetype="application/json"
    )


@app.route('/emotion-quicksearch-data', methods=['GET'])
def getEmotionQuicksearchData():
    if isRenewEnforced(request) or isFullUpdateEnforced(request):
        removeRecords("beam_quicksearch")
        es = Elasticsearch()
        try:
            es.delete_by_query(index="emotion", body={"query": {"match_all": {}}})
        except:
            print("No emotions indexed to elasticsearch to delete, continuing")
        performPipelineOpsForQuickSearchData()
    return Response(
        json_util.dumps(mongo.db.beam_quicksearch.find()),
        mimetype="application/json"
    )


def performPipelineOpsForGlobalEmotionOccurence():
    with beam.Pipeline(runner="DirectRunner") as p:
        (p
         | 'read' >> ReadFromMongoDB(
             uri="mongodb://localhost:27017",
             db="ededore-archive",
             coll="data")
         | 'most occured emotion' >> beam.ParDo(MyParDos.GetMostOccuredEmotion())
         | 'count' >> beam.combiners.Count.PerElement()
         | 'combine' >> beam.CombineGlobally(beam.combiners.ToDictCombineFn())
         | 'add timestamp' >> beam.ParDo(MyParDos.AddTimestamp())
         | 'write' >> beam.io.WriteToMongoDB(
             uri="mongodb://localhost:27017",
             db="ededore-archive",
             coll="beam_emotion_occurence")
         )


def performPipelineOpsForGlobalAbsoluteEmotionOccurences():
    with beam.Pipeline(runner="DirectRunner") as p:
        (p
         | 'read' >> ReadFromMongoDB(uri="mongodb://localhost:27017", db="ededore-archive", coll="data")
         | 'emotions' >> beam.ParDo(MyParDos.ExtractEmotions())
         | 'count' >> beam.combiners.Count.PerElement()
         | 'combine' >> beam.CombineGlobally(beam.combiners.ToDictCombineFn())
         | 'add timestamp' >> beam.ParDo(MyParDos.AddTimestamp())
         | 'write' >> beam.io.WriteToMongoDB(uri="mongodb://localhost:27017", db="ededore-archive", coll="beam_emotion_overall_occurence"))


def performPipelineOpsForAlias(alias):
    with beam.Pipeline(runner="DirectRunner") as p:
        (p
         | 'read' >> ReadFromMongoDB(uri="mongodb://localhost:27017", db="ededore-archive", coll="data")
         | 'filter aliases' >> beam.Filter(hasAlias, alias)
         | 'by alias' >> beam.ParDo(MyParDos.ByAlias())
         | 'group by alias' >> beam.GroupByKey()
         | 'to dict' >> beam.combiners.ToDict()
         | 'add timestamp' >> beam.ParDo(MyParDos.AddTimestamp())
         | 'add alias' >> beam.ParDo(MyParDos.AddAlias(alias))
         | 'write' >> beam.io.WriteToMongoDB(uri="mongodb://localhost:27017", db="ededore-archive", coll="beam_emotion_filtered"))


def performPipelineOpsForQuickSearchData():
    with beam.Pipeline(runner="DirectRunner") as p:
        (p
         | 'read' >> ReadFromMongoDB(
             uri="mongodb://localhost:27017", 
             db="ededore-archive", 
             coll="data")
         | 'extract quicksearch data' >> beam.ParDo(MyParDos.ExtractQuicksearchData())
         | 'write to elasticsearch' >> beam.ParDo(MyParDos.WriteToElasticsearch())
         | 'write' >> beam.io.WriteToMongoDB(
             uri="mongodb://localhost:27017", 
             db="ededore-archive", 
             coll="beam_quicksearch"))


def isFullUpdateEnforced(request):
    force = request.args.get("forceUpdate")
    return force is not None and force.lower() == "full"


def isRenewEnforced(request):
    force = request.args.get("forceUpdate")
    return force is not None and force.lower() == "renew"


def removeOutdatedRecords(collectionName):
    mongo.db[collectionName].remove(
        {"timestamp": {"$lt": datetime.datetime.now() - datetime.timedelta(minutes=2)}}
    )


def removeRecords(collectionName):
    mongo.db[collectionName].remove()
