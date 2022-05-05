
import pulsar
import pandas as pd
import os
class Utils:

    @staticmethod
    def getToken(tokenName):
        token = os.getenv(tokenName)
        return token

    @staticmethod
    def getTokenFromGCP(tokenKey):
        # Do something
        Exception("Not implemented exception:  Utils.getTokenFromGCP(tokenKey)")

    @staticmethod
    def setupPulsarClient(serviceUrl, token):
        token = pulsar.AuthenticationToken(token)
        client = pulsar.Client(service_url=serviceUrl, authentication=token)
        return client
    @staticmethod
    def loadData(fileName, indexCol):
        print("filename is: " + fileName)
        df = pd.read_csv(fileName)
        df = df.set_index([indexCol])
        df = df.reset_index()
        return df