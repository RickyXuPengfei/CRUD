# encoding: utf-8

'''

@author: xupengfei

'''


def new_mysql_connector(connection=None, database=None):
    from .mysql import MySQLConnector
    conf = connection.copy()
    return MySQLConnector(database=database, **conf)


def new_cassandra_connector(connection=None, database=None):
    from .cassandra import CassandraConnetor
    conf = connection.copy()
    return CassandraConnetor(database=database, **conf)
