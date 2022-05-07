import pulsar
from pulsar.schema import *
import hashlib

class Incident(Record):
    _avro_namespace = 'com.trafficcorp.example.demofunctions.function'
    TrafficReportID = String()
    PublishedDate = String()
    IssueReported = String()
    Location = String()
    Latitude = Float(required=True)
    Longitude = Float(required=True)
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
        return AvroSchema(Incident)
