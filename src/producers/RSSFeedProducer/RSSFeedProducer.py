# RSS feed producer

# Create class, methods, make sure it's callable. 


import feedparser # https://universal-feedparser.readthedocs.io/en/latest/

from twisted.internet import task, reactor

from src.common.incident import Incident

url = 'https://www.austintexas.gov/qact/qact_rss.cfm'

timeout = 30.0 # Thirty seconds
client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer(topic='persistent://climate/sandbox/simple-schema', schema=topicSchema)

def doWork():
    # first request
    feed = feedparser.parse(url)

    # store the etag and modified
    last_etag = feed.etag
    last_modified = feed.modified

    # check if new version exists
    feed_update = feedparser.parse(url, etag=last_etag, modified=last_modified)

    if feed_update.status == 304:
        # no changes
    elif feed_update.status == 200:
        # TODO: Produce to Pulsar
        producer.send(Example(a="Sandbox", b=1))
    else:
        # Log an error
    pass

l = task.LoopingCall(doWork)
l.start(timeout) # call every sixty seconds



client.close()

reactor.run()