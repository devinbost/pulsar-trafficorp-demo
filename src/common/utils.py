
import pulsar
import pandas as pd
class Utils:

    @staticmethod
    def getTokenFromGCP(tokenKey):
        # Do something
        return ""

    @staticmethod
    def setupPulsarClient(serviceUrl, token):
        token = pulsar.AuthenticationToken(token)
        client = pulsar.Client(service_url=serviceUrl, authentication=token)
        return client
    @staticmethod
    def loadData(fileName, indexCol):
        df = pd.read_csv(fileName)
        df = df.set_index([indexCol])
        df = df.reset_index()
        return df