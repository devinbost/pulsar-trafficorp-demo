
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

class TestBulkPulsarProducer(unittest.TestCase):

    def test_createObjects(self):
        pulsarProducerMock = Mock()
        sideEffect = lambda value: print(value)
        pulsarProducerMock.send = Mock(return_value=None, side_effect=sideEffect)
        bp = BulkProducer.PulsarBulkProducer('example', 'example')
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/project/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(pulsarProducerMock, df)
        assert len(pulsarProducerMock.mock_calls) == 3
        print(pulsarProducerMock.call_args)

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST1')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://mytenant/default/mytopic')

        for i in range(10):
            producer.send(('Hello World! %d' % i).encode('utf-8'))

        client.close()

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST_SIMPLE')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://austin/default/testme')

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
        producer = client.create_producer('persistent://austin/ingest/traffic-test', schema=mySchema)
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/project/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(producer, df)

        client.close()
    
    def test_bulkTrafficData(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        
        bp = BulkProducer.PulsarBulkProducer('persistent://austin/ingest/traffic-backfill', service_url)
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
        
        rssProducer = RSSFeedProducer.PulsarRssFeedProducer('persistent://austin/ingest/traffic-rss', service_url)
        rssProducer.main('ASTRA_TOKEN_TEST_SIMPLE', 1.0, 10.0)

class TestBulkPulsarJsonProducer(unittest.TestCase):
    def test_createObjects(self):
        pulsarProducerMock = Mock()
        sideEffect = lambda value: print(value)
        pulsarProducerMock.send = Mock(return_value=None, side_effect=sideEffect)
        bp = BulkProducer.PulsarBulkJsonProducer('example', 'example')
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/project/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(pulsarProducerMock, df)
        assert len(pulsarProducerMock.mock_calls) == 3
        print(pulsarProducerMock.call_args)

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST1')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://austin/default/testme')

        for i in range(10):
            producer.send(('Hello World! %d' % i).encode('utf-8'))

        client.close()

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST_SIMPLE')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://austin/default/testme')

        for i in range(10):
            producer.send(('Hello World! %d' % i).encode('utf-8'))

        client.close()
    
    def test_bulkTrafficData(self):
        service_url = 'pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651'
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        
        bp = BulkProducer.PulsarBulkJsonProducer('persistent://austin/default/traffic-backfill-json', service_url)
        bp.main('ASTRA_TOKEN_TEST_BULKLOAD')

class TestAstraDBSetup(unittest.TestCase):

    def test_CreateTable(self):
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        import os

        cwd = os.getcwd()

        cloud_config= {
                'secure_connect_bundle': cwd + '/secure-connect-traffic.zip'
        }
        astraClientId = os.getenv('ASTRA_CLIENT_ID')
        astraSecret = os.getenv('ASTRA_SECRET')
        auth_provider = PlainTextAuthProvider(astraClientId, astraSecret)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        query = "CREATE TABLE incidents.austin ( TrafficReportID text PRIMARY KEY, PublishedDate text, IssueReported text, Location text, Latitude float, Longitude float, Address text, Status text, StatusDate text, Title text);"
        row = session.execute(query).one()
        if row:
            print(row[0])
        else:
            print("An error occurred.")
    def test_SelectAustinIncidents(self):
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        import os

        cwd = os.getcwd()

        cloud_config= {
                'secure_connect_bundle': cwd + '/secure-connect-traffic.zip'
        }
        astraClientId = os.getenv('ASTRA_CLIENT_ID')
        astraSecret = os.getenv('ASTRA_SECRET')
        auth_provider = PlainTextAuthProvider(astraClientId, astraSecret)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        query = "SELECT * FROM incidents.austin;"
        row = session.execute(query).one()
        if row:
            print(row[0])
        else:
            print("An error occurred.")