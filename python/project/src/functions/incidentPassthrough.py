#!/usr/bin/env python3
"""
Pulsar Function that returns the input message. Used for decoupling flows and creating stop valves.
"""
import pulsar
from src.common.incident import Incident
from pulsar.schema import AvroSchema

class Schema:
    schema = None

    def __init__(self, *args):
        self.schema = args[0]

    def __call__(self, f):
        def wrapped(*args):
            args = list(args)
            args[1] = self.schema.decode(args[1].encode())
            return self.schema.encode(f(*tuple(args))).decode("utf-8")

        return wrapped

class IncidentPassthrough(pulsar.Function):
    def __init__(self):
        self.userConfig = None
        self.logger = None
        self.isInitialized = False
    
    def initialize(self, context):
        self.logger = context.get_logger()
        self.userConfig = context.user_config()
        self.isInitialized = True
    
    @Schema(AvroSchema(Incident))
    def process(self, message, context):
        if self.isInitialized == False:
            self.initialize(context)
        return message

