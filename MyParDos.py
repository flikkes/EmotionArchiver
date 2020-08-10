import apache_beam as beam
import datetime
from elasticsearch import Elasticsearch


class GetMostOccuredEmotion(beam.DoFn):
    def process(self, element):
        if "mostOccuredEmotion" in element.keys():
            yield element["mostOccuredEmotion"]


class AddTimestamp(beam.DoFn):
    def process(self, element):
        element["timestamp"] = datetime.datetime.now()
        yield element

class AddAlias(beam.DoFn):
    def __init__(self, alias):
        self.alias = alias
    def process(self, element):
        element["alias"] = self.alias
        yield element


class ExtractEmotions(beam.DoFn):
    def process(self, element, *args, **kwargs):
        emotions = element["emotions"]
        values = []
        for e in emotions:
            values.append(e["emotion"])
        return values

class ExtractQuicksearchData(beam.DoFn):
    def process(self, element):
        newElement = {}
        newElement["emotions"] = element["emotions"]
        newElement["personAlias"] = element["personAlias"]
        newElement["description"] = element["description"]
        newElement["title"] = element["title"]
        yield newElement

class WriteToElasticsearch(beam.DoFn):
    def process(self, element):
        print(element)
        es = Elasticsearch()
        es.index(index="emotion", body=element)
        yield element


class ByAlias(beam.DoFn):
    def process(self, element):
        alias = element.pop("personAlias")
        yield (alias, element)


class LogResults(beam.DoFn):
    def process(self, element):
        print("Pub/Sub event: {}".format(element))
        yield element
