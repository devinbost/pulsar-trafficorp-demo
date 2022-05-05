
import unittest
from unittest.mock import MagicMock, Mock
from unittest.mock import ANY
from src.common.incident import Incident
from src.producers.BulkProducer import BulkProducer
from src.common.utils import Utils
import os
import feedparser
import pulsar

from src.producers.RSSFeedProducer import RSSFeedProducer
# from [directory].producers import RSSProducer?

# Mock pulsar.produce

# Test: BulkProducer.loadData('Real-Time_Traffic_Incident_Reports.csv', 'TrafficReportID')

# Test: BulkProducer.produceData(pulsarProducer, dataframe)
    # Mock: pulsarProducer.send(incident)
class TestBulkPulsarProducer(unittest.TestCase):

    def test_createObjects(self):
        pulsarProducerMock = Mock()
        sideEffect = lambda value: print(value)
        pulsarProducerMock.send = Mock(return_value=None, side_effect=sideEffect)
        bp = BulkProducer.PulsarBulkProducer('example', 'example')
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(pulsarProducerMock, df)
        assert len(pulsarProducerMock.mock_calls) == 3
        print(pulsarProducerMock.call_args)

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST1')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://traffic-corp/default/testme')

        for i in range(10):
            producer.send(('Hello World! %d' % i).encode('utf-8'))

        client.close()

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST_SIMPLE')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://traffic-corp/default/testme')

        for i in range(10):
            producer.send(('Hello World! %d' % i).encode('utf-8'))

        client.close()

    def test_astraConnectionWithSchema(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST_SCHEMA')
        bp = BulkProducer.PulsarBulkProducer(ANY, ANY)
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))
        mySchema = Incident.getIncidentSchema()
        producer = client.create_producer('persistent://traffic-corp/default/testme-schema', schema=mySchema)
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(producer, df)

        client.close()
    
    def test_bulkTrafficData(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        
        bp = BulkProducer.PulsarBulkProducer('persistent://traffic-corp/default/traffic-backfill-schema', service_url)
        bp.main('ASTRA_TOKEN_TEST_BULKLOAD', Incident.getIncidentSchema)
    

class TestRSSPulsarProducer(unittest.TestCase):
    def test_parseFeed(self):
        url = 'https://www.austintexas.gov/qact/qact_rss.cfm'
        feed = feedparser.parse(url)
        feedEntries = feed.entries
        for entry in feedEntries:
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
            incident = Incident(publishedDate, issueReported, latitude, longitude, address, status, statusDate, title)
            
            print(details)
            #test = entry[]
        print(feed)
    
    def test_RSSFeedProducer(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        
        rssProducer = RSSFeedProducer.PulsarRssFeedProducer('persistent://traffic-corp/default/testme', service_url)
        rssProducer.main('ASTRA_TOKEN_TEST_SIMPLE', 1.0, 10.0)
