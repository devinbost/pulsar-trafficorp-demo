#!/usr/bin/env python3
"""
Pulsar Function that returns the input message. Used for decoupling flows and creating stop valves.
"""
import pulsar
from pulsar.schema import *
import hashlib

class Incident(Record):
    _avro_namespace = 'com.trafficcorp.example.demofunctions.function'
    TrafficReportID = String()
    PublishedDate = String()
    IssueReported = String()
    Location = String()
    Latitude = Float()
    Longitude = Float()
    Address = String()
    Status = String()
    StatusDate = String()
    Title = String()

    def __init__(self, publishedDate, issueReported, latitude, longitude, address, status, statusDate, title = 'NULL'):
        combined = address + issueReported + publishedDate
        self.TrafficReportID = hashlib.md5(combined.encode()).hexdigest()
        self.PublishedDate = publishedDate
        self.IssueReported = issueReported
        self.Location = "({0},{0})".format(latitude, longitude)
        self.Latitude = float(latitude)
        self.Longitude = float(longitude)
        self.Address = address
        self.Status = status
        self.StatusDate = statusDate
        self.Title = title
    
    @staticmethod
    def getIncidentSchema():
        return Schema(Incident)


class Schema:
    schema = None

    def __init__(self, *args):
        self.schema = args[0]

    def __call__(self, f):
        def wrapped(*args):
            args = list(args)
            print("args: " + str(args[1]))
            args[1] = self.schema.decode(args[1].encode())
            return self.schema.encode(f(*tuple(args))).decode("utf-8")

        return wrapped

class IncidentPassthrough(pulsar.Function):
    def __init__(self):
        self.userConfig = None
        self.logger = None
        self.isInitialized = False
    
    def initialize(self, context):
        self.logger = context.get_logger()
        self.userConfig = context.user_config()
        self.isInitialized = True
    
    @Schema(AvroSchema(Incident))
    def process(self, message, context):
        if self.isInitialized == False:
            self.initialize(context)
        return message
