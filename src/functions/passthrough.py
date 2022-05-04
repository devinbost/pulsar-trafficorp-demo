#!/usr/bin/env python3
"""
Pulsar Function that returns the input message. Used for decoupling flows and creating stop valves.
"""
import pulsar

class Passthrough(pulsar.Function):
    def __init__(self):
        self.userConfig = None
        self.logger = None
    def process(self, message, context):
        """Process single Pulsar message"""
        self.logger = context.get_logger()
        return message