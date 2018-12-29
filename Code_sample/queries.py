from google.cloud import spanner

instance_id = 'test-instance'
database_id = 'singers-db'
database_id2 = 'sales-db'


def main():
    #query_table_hint_key(instance_id, database_id)
    #join_onprimarykey(instance_id, database_id2)
    #cross_join(instance_id, database_id2)
    #left_join(instance_id, database_id2)
    group_by(instance_id, database_id2)




def query_table_hint_key(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            'SELECT s.SingerId, s.FirstName, s.LastName, s.SingerInfo FROM Singers@{FORCE_INDEX=SingersByFirstLastName} AS s WHERE s.FirstName = "Catalina" AND s.LastName > "M"')

        for row in results:
            print(u'SingerId: {}, AlbumId: {}, AlbumTitle: {}'.format(*row))

def join_onprimarykey(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            'SELECT * FROM Customers JOIN Invoices ON Customers.CustomerId = Invoices.CustomerId')

        for row in results:
            print(row)

def cross_join(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            'SELECT * FROM Customers CROSS JOIN Invoices WHERE Customers.CustomerId = Invoices.CustomerId')

        for row in results:
            print(row)

def left_join(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            'SELECT * FROM Customers LEFT JOIN Invoices ON Customers.CustomerId = Invoices.CustomerId')

        for row in results:
            print(row)

def group_by(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            'SELECT CustomerId, Sum(TotalAmount) FROM Invoices GROUP BY CustomerId')

        for row in results:
            print(row)


main()