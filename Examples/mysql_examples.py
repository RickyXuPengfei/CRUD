from Examples.conf import MYSQL
from crud.connector import new_mysql_connector

if __name__ == '__main__':
    # Read
    connector = new_mysql_connector(database='INSERT DATABASE', connection=MYSQL)
    rows = connector.select(table='INSERT TABLE', fields=['dt', 'category', 'store_name', 'discount_price'],
                            filters=[('dt', '=', '2018-05-09')], order_by=['category'], limit=100)

    # Create
    connector.create(table='INSERT TABLE', data=rows, mode='INSERT')

    # Update
    data = {"discount_price": 110}
    connector.update(table='INSERT TABLE', filters=[('dt', '=', '2018-05-09'), ('store_name', '=', 'xxx')],
                     data=data)

    # Delete
    connector.delete(table='INSERT TABLE', filters=[('store_name', '=', 'xxx')])
