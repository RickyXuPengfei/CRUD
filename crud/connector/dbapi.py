# encoding: utf-8

'''

@author: xupengfei

'''

import pymysql
import logging
import contextlib
from pymysql.cursors import DictCursorMixin, Cursor
from collections import OrderedDict

logger = logging.getLogger(__name__)

class OrderedDictCursor(DictCursorMixin, Cursor):
    dict_type = OrderedDict

class DBApiConnector:
    def __init__(self, host, port, database, row_type=OrderedDict, user=None, password=None, *args, **kwargs):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self.row_type = row_type


    def connect(self,*args,**kwargs):
        return NotImplementedError

    @contextlib.contextmanager
    def cursor(self, autocommit=False):
        conn = self.connect(autocommit=autocommit)
        cursor = conn.cursor(OrderedDictCursor)

        try:
            yield cursor
        finally:
            cursor.close()
            conn.close()

    def execute(self, query, parameter=None):
        if isinstance(query, str):
            query = [query]

        with self.cursor(autocommit=True) as cursor:
            for q in query:
                if parameter is not None:
                    cursor.execute(q, parameter)
                else:
                    cursor.execute(q)

    def executemany(self, query, parameter=None):
        if isinstance(query, str):
            query = [query]

        with self.cursor(autocommit=True) as cursor:
            for q in query:
                if parameter is not None:
                    cursor.executemany(q, parameter)
                else:
                    cursor.executemany(q)

    def fetchall(self, query, parameters = None):
        with self.cursor() as cursor:
            if parameters is not None:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            rows = cursor.fetchall()
        return rows

    def fetchone(self,query,parameters=None):
        with self.cursor() as cursor:
            if parameters is not None:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            row = cursor.fetchone()
        return row






