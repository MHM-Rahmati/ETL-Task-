# package structure for further improvments
from config import *

class dbUtils :
    def __init__(self, ConnectionString):
        # self.session = connect(ConnectionString)
        pass
    
    @staticmethod
    def connect(cs):
        # session = db_library_connection_method(cs)
        # return session
        pass
    
    def get_results(self, query):
        try:
            # result = self.session.db_library_query_method(query)
            pass
        except Exception as e:
            return f"Error : {e}"
