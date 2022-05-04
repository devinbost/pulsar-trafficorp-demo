
import pandas as pd
import pandavro as pda
import pulsar
from pulsar.schema import *
import avro

# Pandavro library can be used if it would be preferred to infer schema from CSV via Pandas

# TODO: Pull input topic from startup parameter

client = pulsar.Client('pulsar://localhost:6650')


df = loadData('Real-Time_Traffic_Incident_Reports.csv', 'TrafficReportID')
pda.to_avro('incidents.avro', df)
avroSchema = avro.schema.parse(open("incidents.avro", "rb").read())

producer = client.create_producer(topic='persistent://austin/ingest/traffic-backfill', schema=avroSchema)

for index, row in df.itertuples(index=True):
    incident = Incident(row.TrafficReportID, row.PublishedDate, row.IssueReported, row.Location, row.Latitude, row.Longitude, row.Address, row.Status, row.StatusDate)
    producer.send(incident)


client.close()

def loadData(self, fileName, indexCol):
    df = pd.read_csv(fileName)
    df = df.set_index([indexCol])
    df = df.reset_index()
    return df