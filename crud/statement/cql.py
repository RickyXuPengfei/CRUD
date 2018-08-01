# encoding: utf-8

'''

@author: xupengfei

'''

from cassandra.cqlengine.operators import BaseWhereOperator
from cassandra.cqlengine.statements import (
    InsertStatement,
    UpdateStatement,
    SelectStatement as _SelectStatement,
    DeleteStatement,
    WhereClause, AssignmentClause,
    six
)


class SelectStatement(_SelectStatement):
    # __unicode__ (python 2) == __str__ (python3)
    def __unicode__(self):
        qs = ['SELECT']
        if self.distinct_fields:
            distinct_fields_str = ', '.join(['"{0}"'.format(f) for f in self.distinct_fields])
            if self.count:
                qs += ['DISTINCT COUNT({})'.format(distinct_fields_str)]
            else:
                qs += ['DISTINCT {}'.format(distinct_fields_str)]
        elif self.count:
            qs += ['COUNT(*)']
        else:
            if self.fields:
                fields_str = ', '.join(['"{0}"'.format(f) for f in self.fields])
            else:
                fields_str = '*'
            qs += [fields_str]
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += [self._where]

        if self.order_by and not self.count:
            segs = []
            for order_col in self.order_by:
                if order_col.startswith('-'):
                    segs.append(six.text_type(order_col[1:]) + 'DESC')
                else:
                    segs.append(six.text_type(order_col))
            order_by_str = f"ORDER BY {', '.join(segs)}"
            qs += [order_by_str]

        if self.limit:
            qs += [f'LIMIT {self.limit}']

        if self.allow_filtering:
            qs += ["ALLOW FILTERING"]

        return ' '.join(qs)

CQL_SYMBOL = dict(
    [(op.cql_symbol, op.symbol) for op in BaseWhereOperator.opmap.values()]
)

def generate_where_clause_from_filters(filters: list):
    where_clauses = []
    for k, op, v in filters:
        cql_op = CQL_SYMBOL.get(op.upper(),None)
        if not cql_op:
            raise ValueError("Unsupported Operation")
        where_clause = WhereClause(k, BaseWhereOperator.get_operator(cql_op)(), v)
        where_clauses.append(where_clause)
    return where_clauses

def assignment_clauses_from_data(data:dict):
    assignment_clauses = []
    for k, v in data.items():
        assignment_clauses.append(AssignmentClause(k, v))
    return assignment_clauses

def insert_query(table:str, data:dict, mode:str='INSERT'):
    assignment_clauses = assignment_clauses_from_data(data)
    if mode == 'REPLACE':
        statement = InsertStatement(
            table, assignment_clauses
        )
    else:
        statement = InsertStatement(
            table, assignment_clauses,
            if_not_exists=True
        )
    return str(statement), statement.get_context()

def update_query(table:str, filters:list, data:dict):
    where_clauses = generate_where_clause_from_filters(filters)
    assignment_clauses = assignment_clauses_from_data(data)
    statement = UpdateStatement(
        table, assignment_clauses,
        where_clauses, if_exists=True
    )
    return str(statement), statement.get_context()

def delete_query(table:str, filters:list):
    where_clauses = generate_where_clause_from_filters(filters)
    statement = DeleteStatement(
        table, where_clauses
    )
    return str(statement), statement.get_context()

def select_query(
    collection, filters, fields=None, order_by=None, limit=None
):
    where_clauses = generate_where_clause_from_filters(filters)
    statement = SelectStatement(
        table=collection, fields=fields,
        where=where_clauses, allow_filtering=True,
        order_by=order_by, limit=limit
    )
    return str(statement), statement.get_context()

if __name__ == '__main__':
    c = insert_query('test', {'a': 1, 'b': 2}, 'REPLACE')
    # print (c)
    r = select_query('table', [('a', '>', 1), ('b', '=', 2)], ['a', 'b'])
    # print (r)
    u = update_query('test', [('t1', '>', 1), ('t2', '=', 1)], {'a': 1, 'b': 2})
    # print (u)
    d = delete_query('test', [('a', '>', 1), ('b', '=', 2)])
    # print(d)
