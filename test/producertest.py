
import unittest
from unittest.mock import MagicMock, Mock
from src.producers.BulkProducer import BulkProducer
from src.common.utils import Utils
import os
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
        bp = BulkProducer.BulkProducerObj('persistent://austin/ingest/traffic-backfill', 'pulsar://localhost:6650')
        cwd = os.getcwd()
        df = Utils.loadData(cwd + "/test/incident_sample.csv",  'TrafficReportID')
        bp.produceData(pulsarProducerMock, df)
        assert len(pulsarProducerMock.mock_calls) == 3
        print(pulsarProducerMock.call_args)
