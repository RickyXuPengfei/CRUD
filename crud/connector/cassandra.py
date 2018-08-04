# encoding: utf-8

'''

@author: xupengfei

'''
import logging

import retrying
from cassandra import ReadTimeout
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, default_lbp_factory
from cassandra.policies import RetryPolicy, ConstantReconnectionPolicy
from cassandra.query import ordered_dict_factory

from crud.statement.cql import (
    select_query,
    insert_query,
    delete_query,
    update_query
)

logger = logging.getLogger(__name__)


class ClosingSession:
    def __init__(self, session):
        self._session = session
        self._cluster = session.cluter

    def __getattr__(self, name):
        return getattr(self._session, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.shutdown()
        self._cluster.shutdown()


class CassandraConnetor:
    def __init__(self, host, port, database=None, user=None, password=None, *args, **kwargs):
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.args = args
        self.kwargs = kwargs

    def connect(self, *args, **kwargs):
        auth = PlainTextAuthProvider(username=self.user, password=self.password)
        cluster = Cluster(
            contact_points=self.host,
            auth_provider=auth,
            protocol_version=3,
            load_balancing_policy=default_lbp_factory(),
            default_retry_policy=RetryPolicy(),
            reconnection_policy=ConstantReconnectionPolicy(delay=1, max_attempts=10),
            *args, **kwargs
        )
        return cluster

    def session(self, *args, **kwargs):
        cluster = self.connect(*args, **kwargs)
        return cluster.connect(self.database)

    def closing_session(self, *args, **kwargs):
        real_session = self.session(*args, **kwargs)
        real_session.row_factory = ordered_dict_factory
        session = ClosingSession(real_session)
        return session

    def execute(self, query, parameters=None, timeout=20, retry=3):
        with self.closing_session() as session:
            retry_handler = retrying.Retrying(retry_on_exception=_retry_if_timeout,
                                              stop_max_attempt_number=retry)
            return retry_handler.call(_execute_query, session, query, parameters, timeout)

    def _create_one(self, table, data, mode='INSERT', **kwargs):
        query, params = insert_query(table, data, mode)
        try:
            self.execute(query, params, **kwargs)
        except ValueError as e:
            logger.info(e)

    def _create_many(self, table, datas, mode='INSERT', **kwargs):
        if not isinstance(datas, (list, tuple)):
            datas = [datas]
        data_one = datas[0]
        query, _ = insert_query(table, data_one, mode)
        params = [list(data.values()) for data in datas]
        for param in params:
            try:
                self.execute(query, param, **kwargs)
            except ValueError as e:
                logger.info(e)

    def create(self, table, data, mode="INSERT", **kwargs):
        if isinstance(data, dict):
            self._create_one(table, data, mode, **kwargs)
        else:
            self._create_one(table, data, mode, **kwargs)

    def update(self, table, data, filters, **kwargs):
        query, params = update_query(table, filters, data)
        self.execute(query, params, **kwargs)

    def select(self, table, fields, filters, order_by, limit, **kwargs):
        query, params = select_query(table, fields, filters, order_by, limit)
        rows = self.execute(query, params, **kwargs)
        return rows

    def delete(self, table, filters, **kwargs):
        query, params = delete_query(table, filters)
        self.execute(query, params, **kwargs)


def _retry_if_timeout(exc):
    return isinstance(exc, ReadTimeout)


def _execute_query(session, query, parameters, timeout, *args, **kwargs):
    rows = session.execute(query, parameters, timeout=timeout, *args, **kwargs)
    results = list(rows)
    return results
