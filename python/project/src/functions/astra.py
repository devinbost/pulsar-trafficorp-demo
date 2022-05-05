import pulsar

class AstraLTWriter(pulsar.Function):
    def __init__(self):
        self.userConfig = None
        self.logger = None
    def process(self, message, context):
        """Process single Pulsar message"""
        self.logger = context.get_logger()

        # TODO: Get Astra token & url from context
        # TODO: Query Astra for GUID. If exists, then write record to Astra and return it
        #       Else, return null.

        return message