# CURD

## Description

CURD is a database client realizing crud operation for Cassandra and Mysql.

Supported operations: `create` `select` `update` `delete`

Supported Data Source: `Cassandra` `Mysql`  `Hive` (COMING SOON)

### Examples

#### Read

```python
connector = new_mysql_connector(database='INSERT DATABASE', connection=MYSQL)
rows = connector.select(table='INSERT TABLE', fields=['dt', 'category', 'store_name', 'discount_price'],
                        filters=[('dt', '=', '2018-05-09')], order_by=['category'], limit=100)
```

```python
[OrderedDict([('dt', datetime.date(2018, 5, 9)),
              ('category', 'babycare'),
              ('store_name', 'moony京东官方旗舰店'),
              ('discount_price', 109.0)]),
 OrderedDict([('dt', datetime.date(2018, 5, 9)),
              ('category', 'babycare'),
              ('store_name', 'Pampers京东自营旗舰店'),
              ('discount_price', 115.0)]),
 OrderedDict([('dt', datetime.date(2018, 5, 9)),
              ('category', 'babycare'),
              ('store_name', '菲比自营旗舰店'),
              ('discount_price', 113.85)]),
 OrderedDict([('dt', datetime.date(2018, 5, 9)),
              ('category', 'babycare'),
              ('store_name', '好奇自营官方旗舰店'),
              ('discount_price', 142.29)]),
 OrderedDict([('dt', datetime.date(2018, 5, 9)),
              ('category', 'babycare'),
              ('store_name', '好奇自营官方旗舰店'),
              ('discount_price', 134.16)])]
```



#### Create

```python
connector.create(table='INSERT TABLE', data=rows, mode='INSERT')
```



#### Update

```python
data = {"discount_price": 110}
    connector.update(table='INSERT TABLE', filters=[('dt', '=', '2018-05-09'), ('store_name', '=', 'xxx')],
                     data=data)
```



#### Delete

```python
connector.delete(table='INSERT TABLE', filters=[('store_name', '=', 'xxx')])
```

