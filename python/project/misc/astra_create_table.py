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
query = "CREATE TABLE incident ( TrafficReportID text PRIMARY KEY, PublishedDate text, IssueReported text, Location text, Latitude float, Longitude float, Address text, Status text, StatusDate text, Title text);"
row = session.execute(query).one()
if row:
    print(row[0])
else:
    print("An error occurred.")