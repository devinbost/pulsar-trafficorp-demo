
import pandas as pd
import pulsar
from pulsar.schema import *
import logging
import os

from src.common.incident import Incident
from src.common.utils import Utils

# Pandavro library can be used if it would be preferred to infer schema from CSV via Pandas

# TODO: Get token from secret manager

class BulkProducerObj:
    def __init__(self, topic, url):
        self.logger = logging.getLogger("mylogger")
        self.myToken = Utils.getTokenFromGCP("myToken")
        self.myTopic = topic # e.g. topic = 'persistent://austin/ingest/traffic-backfill'
        self.serviceUrl = url # e.g. url = 'pulsar://localhost:6650'

    def produceData(self, pulsarProducer, dataframe):
        for row in dataframe.itertuples(index=True):
            incident = Incident(row.TrafficReportID, row.PublishedDate, row.IssueReported, row.Location, row.Latitude, row.Longitude, row.Address, row.Status, row.StatusDate)
            pulsarProducer.send(incident)

    def main(self):
        cwd = os.getcwd()
        df = Utils.loadData(cwd + '/src/producers/BulkProducer/Real-Time_Traffic_Incident_Reports.csv', 'TrafficReportID')
        try:
            client = Utils.setupPulsarClient(self.serviceUrl, self.myToken)
            producer = client.create_producer(topic=self.myTopic, schema=AvroSchema(Incident))
        except Exception:
            self.logger.exception("Unable to connect to Pulsar topic: {} at serviceUrl: {} ".format(self.myTopic, self.serviceUrl))
        self.logger.info("Producer connected")

        self.produceData(producer, df)

        client.close()
