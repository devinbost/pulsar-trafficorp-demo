# RSS feed producer

# Create class, methods, make sure it's callable. 


import feedparser # https://universal-feedparser.readthedocs.io/en/latest/
import logging
import pulsar
import time
from src.common.incident import Incident
from src.common.utils import Utils

class PulsarRssFeedProducer:
    def __init__(self, topic: str, url: str):
        self.logger = logging.getLogger("RSSlogger")
        self.pulsarTopic = topic # e.g. topic = 'persistent://austin/ingest/traffic-rss'
        self.serviceUrl = url # e.g. url = 'pulsar://localhost:6650'
        self.timeout = 30 # 30 seconds
        self.lastEntry = None

    def lastEntryGuid(self):
        if self.lastEntry is None:
            return None
        else:
            return self.lastEntry.guid
    
    def produceEntriesAsync(self, pulsarProducer, entries):
        for entry in entries:
            publishedDate = entry["published"]
            title = entry["title"]
            splitDetails = entry["summary"].split('|')
            details = [x.strip() for x in splitDetails if not splitDetails == '']
            if len(details) < 5:
                print("WARNING: Skipping incomplete record")
                continue
            address = details[0]
            latitude = details[1]
            longitude = details[2]
            issueReported = details[3]
            status = "NULL"
            statusDate = "NULL"
            try:
                incident = Incident(publishedDate, issueReported, latitude, longitude, address, status, statusDate, title)
                pulsarProducer.send_async(incident, callback=self.send_callback)
            except TypeError as err:
                print("TypeError for entry: {}. Error is: {}".format(entry, err))
                raise
        pulsarProducer.flush()

    def send_callback(self, res, msg):
        print('Message published res=%s', res)

    def periodicCheck(self, pulsarProducer, url):
        feed_update = feedparser.parse(url)
        feedEntries = feed_update.entries
        areEntriesNew = self.lastEntryGuid() != feedEntries[0].guid
        if self.lastEntryGuid() is None or areEntriesNew:  
            # Process the new data:
            self.produceEntriesAsync(pulsarProducer, feedEntries)
            self.lastEntry = feedEntries[0] # Not thread safe! Fix if you make this multi-threaded.
        else:
            self.logger.info("No changes")

    def main(self, tokenName: str, interval: float, timeout: float):
        """timeout and interval are in seconds. For no timeout, set timeout = 0"""
        rssUrl = 'https://www.austintexas.gov/qact/qact_rss.cfm'
        token = Utils.getToken(tokenName)
        client = Utils.setupPulsarClient(self.serviceUrl, token)
        producer = client.create_producer(topic=self.pulsarTopic, 
                schema=Incident.getIncidentSchema())
        endTime = time.time() + timeout
            
        while True:
            print("tick")
            try:
                self.periodicCheck(producer, rssUrl)
            except:
                client.close()
                raise
            if timeout > 0:
                if time.time() > endTime:
                    client.close()
                    break
            time.sleep(interval)
        

