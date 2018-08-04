# encoding: utf-8

'''

@author: xupengfei

'''

import logging

import pymysql

from crud.connector.dbapi import DBApiConnector
from crud.statement.sql import (
    insert_query,
    select_query,
    update_query,
    delete_query
)

logger = logging.getLogger(__name__)


class MySQLConnector(DBApiConnector):
    def connect(self, autocommit=False, *args, **kwargs):
        return pymysql.connect(
            host=self.host,
            port=self.port or 3306,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8',
            autocommit=autocommit,
            *args,
            **kwargs
        )

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
        params = [tuple(data.values()) for data in datas]
        try:
            self.executemany(query, params, **kwargs)
        except ValueError as e:
            logger.info(e)

    def create(self, table, data, mode="INSERT", **kwargs):
        logger.info(f"Start to create")
        if isinstance(data, dict):
            self._create_one(table, data, mode, **kwargs)
        else:
            self._create_many(table, data, mode, **kwargs)
        logger.info(f"Create Done. In {table}")

    def update(self, table, data, filters, **kwargs):
        logger.info(f"Update {table}")
        query, params = update_query(table, filters, data)
        self.execute(query, params, **kwargs)
        logger.info(f"Update Done")

    def select(self, table, fields, filters, order_by, limit=None, **kwargs):
        logger.info(f"Start to read from {table}")
        query, params = select_query(table, fields, filters, order_by, limit)
        rows = self.fetchall(query, params, **kwargs)
        logger.info(f"Read Done")
        return rows

    def delete(self, table, filters, **kwargs):
        logger.info(f"Start to delete from {table}")
        query, params = delete_query(table, filters)
        self.execute(query, params, **kwargs)
        logger.info(f"Delete Done")
