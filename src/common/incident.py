import pulsar
from pulsar.schema import *

class Incident(Record):
    _avro_namespace = 'consoto.traffic.incident'
    TrafficReportID = String()
    PublishedDate = String()
    IssueReported = String()
    Location = String()
    Latitude = Float()
    Longitude = Float()
    Address = String()
    Status = String()
    StatusDate = String()

    def __init__(self, trafficReportId, publishedDate, issueReported, location, latitude, longitude, address, status, statusDate):
        self.TrafficReportID = trafficReportId
        self.PublishedDate = publishedDate
        self.IssueReported = issueReported
        self.Location = location
        self.Latitude = latitude
        self.Longitude = longitude
        self.Address = address
        self.Status = status
        self.StatusDate = statusDate