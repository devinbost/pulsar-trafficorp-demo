
import unittest
from unittest.mock import MagicMock, Mock
from unittest.mock import ANY
from src.producers.BulkProducer import BulkProducer
from src.common.utils import Utils
import os
import pulsar
# from [directory].producers import RSSProducer?

# Mock pulsar.produce

# Test: BulkProducer.loadData('Real-Time_Traffic_Incident_Reports.csv', 'TrafficReportID')

# Test: BulkProducer.produceData(pulsarProducer, dataframe)
    # Mock: pulsarProducer.send(incident)
class TestPulsarProducers(unittest.TestCase):

    def test_createObjects(self):
        pulsarProducerMock = Mock()
        sideEffect = lambda value: print(value)
        pulsarProducerMock.send = Mock(return_value=None, side_effect=sideEffect)
        bp = BulkProducer.BulkProducerObj('example', 'example')
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(pulsarProducerMock, df)
        assert len(pulsarProducerMock.mock_calls) == 3
        print(pulsarProducerMock.call_args)

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST1')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://contoso-corp/default/testme')

        for i in range(10):
            producer.send(('Hello World! %d' % i).encode('utf-8'))

        client.close()

    def test_astraConnection(self):
        service_url = 'pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST_SIMPLE')
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))

        producer = client.create_producer('persistent://contoso-corp/default/testme')

        for i in range(10):
            producer.send(('Hello World! %d' % i).encode('utf-8'))

        client.close()

    def test_astraConnectionWithSchema(self):
        service_url = 'pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651'
        token = os.getenv('ASTRA_TOKEN_TEST_SCHEMA')
        bp = BulkProducer.BulkProducerObj(ANY, ANY)
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        client = pulsar.Client(service_url,
                                authentication=pulsar.AuthenticationToken(token))
        mySchema = bp.getSchema()
        producer = client.create_producer('persistent://contoso-corp/default/testme-schema', schema=mySchema)
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(producer, df)

        client.close()
    
    def test_bulkTrafficData(self):
        service_url = 'pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651'
        # Note: To setup env vars, create .env file in root project dir. That's where it gets picked up.
        
        bp = BulkProducer.BulkProducerObj('persistent://austin/ingest/traffic-backfill', service_url)
        bp.main('ASTRA_TOKEN_TEST_BULKLOAD')