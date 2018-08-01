# encoding: utf-8

'''

@author: xupengfei

'''

import datetime

from utils import quote_identifier, parse_local_time


class BaseClause:
    def __init__(self, field: str, value):
        self._field = field
        self._value = value

    @property
    def field(self):
        field = quote_identifier(self._field)
        return field

    @property
    def value(self):
        if isinstance(self._value, datetime.datetime):
            value = parse_local_time(self._value)
            return value
        elif isinstance(self._value, (list, tuple, set)):
            value = []
            for v in self._value:
                if isinstance(v, datetime.datetime):
                    value.append(parse_local_time(v))
                else:
                    value.append(v)
            return value
        else:
            return self._value


class WhereClause(BaseClause):
    """
    a single where statement used in queries
    """
    def __init__(self, field: str, operator: str, value):
        super().__init__(field, value)
        self._operator = operator
        if value is None:
            self._operator = self._operator.map({
                '=': 'IS', '!=': 'IS NOT'
            })

    @property
    def operator(self):
        return self._operator

    def __str__(self):
        return ' '.join([self.field, self.operator, self.value])


class BaseSQLStatement:
    def __init__(self):
        self.query = None
        self.params = []

    def generate_query_where(self, where: list):
        query = ''

        if where:
            query += 'WHERE'
            segs = []
            if not isinstance(where, (list, tuple)):
                where = [where]
            for where_clause in where:
                if where_clause.operator == 'IN':
                    placeholders = ', '.join(['%s' for i in range(len(where_clause.value))])
                    where_placeholder = f'({placeholders})'
                    where_query = f'{where_clause.field} {where_clause.operator} {where_placeholder}'
                    segs.append(where_query)
                    for value in self.where_clause.value:
                        self.params.append(value)
                else:
                    where_query = f'{where_clause.field} {where_clause.operator}  %s'
                    segs.append(where_query)
                    self.params.append(where_clause.value)
            query += ' AND '.join(segs)
        return query

    @staticmethod
    def generate_query_table(table: str):
        return quote_identifier(table)

    @staticmethod
    def generate_query_fields(fields: list):
        if fields:
            fields = map(lambda x: quote_identifier(x), fields)
            return ', '.join(fields)
        else:
            return '*'

    @staticmethod
    def generate_query_limit(limit: str):
        if limit:
            return f'LIMIT {limit}'
        else:
            return ''

    @staticmethod
    def generate_query_order_by(order_by: list):
        query = ''
        if order_by:
            query += 'ORDER BY '
            segs = []
            order_by = map(lambda x: quote_identifier(x), order_by)
            for field in order_by:
                if field.startswith('-'):
                    field = field[1:]
                    seg = field + ' DESC'
                elif field[1] == '-':
                    field = field[2:]
                    seg = field + ' DESC'
                else:
                    seg = field
                segs.append(seg)
            query += ', '.join(segs)
        return query

    @staticmethod
    def generate_query_mode(mode: str):
        if mode == 'INSERT':
            return 'INSERT'
        elif mode == 'IGNORE':
            return 'INSERT IGNORE'
        elif mode == 'REPLACE':
            return "REPLACE"
        else:
            raise ValueError("Unsupported mode")


class SelectStatement(BaseSQLStatement):
    """ a sql select statement """

    BASE_QUERY = 'SELECT {} FROM {} {}'

    def __init__(self, table: str, fields: list = None, where: list = None, order_by: list = None, limit=None):
        super().__init__()

        self.table = table
        self.fields = fields
        self.where = where
        self.order_by = order_by
        self.limit = limit

    def as_sql(self):
        query_table = self.generate_query_table(self.table)
        query_fields = self.generate_query_fields(self.fields)
        query_where = self.generate_query_where(self.where)
        query_order_by = self.generate_query_order_by(self.order_by)
        query_limit = self.generate_query_limit(self.limit)

        extra_query = ' '.join(i for i in [query_where, query_order_by, query_limit] if i)

        self.query = self.BASE_QUERY.format(query_fields, query_table, extra_query)

        return self.query, self.params


class DeleteStatement(BaseSQLStatement):
    """ a sql delete statement """

    BASE_QUERY = 'DELETE FROM {} {}'

    def __init__(self, table: str, where: list = None):
        super().__init__()
        self.table = table
        self.where = where

    def as_sql(self):
        query_table = self.generate_query_table(self.table)
        query_where = self.generate_query_where(self.where)

        self.query = self.BASE_QUERY.format(
            query_table, query_where
        )
        return self.query, self.params


class InsertStatement(BaseSQLStatement):
    """ a sql insert statement """

    BASE_QUERY = '{} INTO {} ({}) VALUES ({})'

    def __init__(self, table: str, assignments: list, mode: str):
        super(InsertStatement, self).__init__()
        self.table = table
        self.assignments = assignments
        self.mode = mode

    def generate_query_fields_values(self, assignments: list):
        query_fields = ', '.join([a.field for a in assignments])
        query_values = ', '.join(['%s'] * len(assignments))
        for a in assignments:
            self.params.append(a.value)
        return query_fields, query_values

    def as_sql(self):
        query_mode = self.generate_query_mode(self.mode)
        query_table = self.generate_query_table(self.table)
        query_fields, query_values = self.generate_query_fields_values(
            self.assignments)

        self.query = self.BASE_QUERY.format(
            query_mode, query_table, query_fields, query_values
        )
        return self.query, self.params


class UpdateStatement(BaseSQLStatement):
    """ a sql update statement """

    BASE_QUERY = 'UPDATE {} SET {} {}'

    def __init__(self, table: str, assignments: list, where: list = None):
        super().__init__()
        self.table = table
        self.assignments = assignments
        self.where = where

    def generate_query_fields_values(self, assignments: list):
        query = ', '.join([a.field + '=%s' for a in assignments])
        for a in assignments:
            self.params.append(a.value)
        return query

    def as_sql(self):
        query_table = self.generate_query_table(self.table)
        query_fields_values = self.generate_query_fields_values(self.assignments)
        query_where = self.generate_query_where(self.where)

        self.query = self.BASE_QUERY.format(
            query_table, query_fields_values, query_where
        )

        return self.query, self.params


def generate_where_clause_from_filters(filters: list):
    if filters is None or len(filters) == 0:
        return ''
    return [WhereClause(*f) for f in filters]


def assignment_clauses_from_data(data: dict):
    assignment_clauses = []
    for k, v in data.items():
        clause = BaseClause(k, v)
        assignment_clauses.append(clause)
    return assignment_clauses


def insert_query(table: str, data: dict, mode='INSERT'):
    assignments = assignment_clauses_from_data(data)
    query, params = InsertStatement(table, assignments, mode).as_sql()
    return query, params


def update_query(table: str, filters: list, data: dict):
    assignments = assignment_clauses_from_data(data)
    where = generate_where_clause_from_filters(filters)
    query, params = UpdateStatement(table, assignments, where).as_sql()
    return query, params


def select_query(table: str, fields: list, filters: list=None, order_by:list=None, limit:int=None):
    where = generate_where_clause_from_filters(filters)
    if not fields:
        fields = []
    if not isinstance(fields, (list, tuple)):
        fields = [fields]
    query, params = SelectStatement(table, fields, where, order_by ,limit=limit).as_sql()
    return query, params


def delete_query(table: str, filters: list):
    where = generate_where_clause_from_filters(filters)
    query, params = DeleteStatement(table, where).as_sql()
    return query, params


if __name__ == '__main__':
    c = insert_query('test', {'a': 1, 'b': 2}, 'INSERT')
    # print (c)
    # r = select_query('table', ['a', 'b'],[('a', '>', 1), ('b', '=', 2)], ['a','-c'])
    # print (r)
    # u = update_query('test', [('t1', '>', 1), ('t2', '=', 1)], {'a': 1, 'b': 2})
    # print (u)
    # d = delete_query('test', [('a', '>', 1), ('b', '=', 2)])
    # print(d)
