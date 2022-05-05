#!/usr/bin/env python3
"""
Pulsar Function that returns the input message. Used for decoupling flows and creating stop valves.
This function can be deployed by simply uploading just this file. 
(No need to zip it for deployment since it has no dependencies outside of pulsar.)
"""
import pulsar

class Passthrough(pulsar.Function):
    def __init__(self):
        self.userConfig = None
        self.logger = None
        self.isInitialized = False
    def initialize(self, context):
        self.logger = context.get_logger()
        self.logger.info("Context is: {}".format(context))
        self.userConfig = context.get_user_config_map()
        self.isInitialized = True
    def process(self, message, context):
        if self.isInitialized == False:
            self.initialize(context)
        return message