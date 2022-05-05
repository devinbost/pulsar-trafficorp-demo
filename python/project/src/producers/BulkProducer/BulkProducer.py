
import pandas as pd
import pulsar
from pulsar.schema import AvroSchema
import logging
import os

from src.common.incident import Incident
from src.common.utils import Utils

# Pandavro library can be used if it would be preferred to infer schema from CSV via Pandas

# TODO: Get token from secret manager

class PulsarBulkProducer:
    def __init__(self, topic, url):
        self.logger = logging.getLogger("mylogger")
        self.myTopic = topic # e.g. topic = 'persistent://austin/ingest/traffic-backfill'
        self.serviceUrl = url # e.g. url = 'pulsar://localhost:6650'

    def produceData(self, pulsarProducer, dataframe):
        for row in dataframe.itertuples(index=True):
            incident = Incident(row.PublishedDate, row.IssueReported, row.Latitude, row.Longitude, row.Address, row.Status, row.StatusDate)
            pulsarProducer.send(incident)
    
    def produceDataAsync(self, pulsarProducer, dataframe):
        for row in dataframe.itertuples(index=True):
            try:
                incident = Incident(row.PublishedDate, row.IssueReported, row.Latitude, row.Longitude, row.Address, row.Status, row.StatusDate)
                pulsarProducer.send_async(incident, callback=self.send_callback)
            except TypeError as err:
                print("TypeError for row: {}. Error is: {}".format(row, err))
                raise
        pulsarProducer.flush()
    
    def send_callback(self, res, msg):
        print('Message published res=%s', res)
    
    def main(self, tokenName, getSchemaMethod):
        cwd = os.getcwd()
        df = Utils.loadData(cwd + '/src/producers/BulkProducer/Real-Time_Traffic_Incident_Reports.csv', 'TrafficReportID')
        try:
            token = Utils.getToken(tokenName)
            client = Utils.setupPulsarClient(self.serviceUrl, token)
            producer = client.create_producer(topic=self.myTopic, 
                schema=getSchemaMethod(),
                batching_enabled=True,
                batching_max_publish_delay_ms=300) # Increasing batch size to improve throughput
            
        except Exception:
            self.logger.exception("Unable to connect to Pulsar topic: {} at serviceUrl: {} ".format(self.myTopic, self.serviceUrl))
            raise
        self.logger.info("Producer connected")

        self.produceDataAsync(producer, df)

        client.close()
