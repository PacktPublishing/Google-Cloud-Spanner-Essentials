from google.cloud import spanner

instance_id = 'test-instance' #id of the spanner instance
database_id = 'singers-db' #id of the spanner Database
database_id2= 'sales-db'

def main():
    #batch_insert(instance_id,database_id)
    #dml_insert(instance_id, database_id)
    #insert_data(instance_id, database_id)
    batch_insert(instance_id, database_id2)
    dml_insert(instance_id, database_id2)
    update_data(instance_id, database_id)
# [START
  # spanner_insert_data]
def insert_data(instance_id, database_id):
    """Inserts sample data into the given database.

    The database and table must already exist and can be created using
    `create_database`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table='Singers',
            columns=('SingerId', 'FirstName', 'LastName',),
            values=[
                (1, u'Marc', u'Richards'),
                (2, u'Catalina', u'Smith'),
                (3, u'Alice', u'Trentor'),
                (4, u'Lea', u'Martin'),
                (5, u'David', u'Lomond')])

        batch.insert(
            table='Albums',
            columns=('SingerId', 'AlbumId', 'AlbumTitle',),
            values=[
                (1, 1, u'Total Junk'),
                (1, 2, u'Go, Go, Go'),
                (2, 1, u'Green'),
                (2, 2, u'Forever Hold Your Peace'),
                (2, 3, u'Terrified')])

    print('Inserted data.')
# [END spanner_insert_data]

def batch_insert(instance_id, database_id):

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table='Customers',
            columns=('CustomerId','Email', 'FirstName', 'LastName',),
            values=[
                (1,u'halle1@gmail.com',   u'Halle', u'Rojer'),
                (2,u'catty@hotmail.com',  u'Caty',  u'Smith'),
                (3,u'asma@hotmail.com',   u'Asma',  u'Aziz'),
                (4,u'martin@gmail.com',   u'Ash',   u'Martin'),
                (5,u'ayesha92@gmail.com', u'Ayesha',u'Saint')])

        batch.insert(
            table='Invoices',
            columns=('CustomerId', 'InvoiceId', 'TotalAmount',),
            values=[
                (1, 1, 225),
                (2, 1, 15),
                (3, 1, 75),
                (3, 2, 60),
                (5, 1, 50)])
        batch.insert(
            table='LineItems',
            columns=('CustomerId', 'InvoiceId', 'LineItemId','ProductId','Quantity',),
            values=[
                (1, 1,1,5,2),
                (1, 1,2,3,7),
                (2, 1,1,1,3),
                (3, 1,1,5,3),
                (3, 2,1,2,2),
                (5, 1,1,5,2)])
                
        batch.insert(
            table='Products',
            columns=('ProductId','Item', 'ProductDesc', 'ProductPrice',),
            values=[
                (1,u'Steel Watch', u'blue color',5),
                (2,u'Table clock', u'has an alarm',30),
                (3,u'Wrist Watch', u'trendy', 25),
                (4,u'Sun glasses', u'black frame', 70),
                (5,u'Table cloth', u'white color',25),
                (6,u'lady shoes',  u'pink', 40),
                (7,u'jacket',      u'leather', 30)])

    print('Inserted data.')
def dml_insert(instance_id, database_id):
    #Use data manipulation language to insert data into DB

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def insert_data(transaction):
        row = transaction.execute_update(
            "INSERT Customers (CustomerId, Email, FirstName, LastName) VALUES"
            "(6, 'josh@gmail.com', 'Josh', 'Wisconsin')"
        )

        print("{} record(s) inserted.".format(row))

    database.run_in_transaction(insert_data)
    

def query_data(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            'SELECT CustomerId, Email, FirstName, LastName FROM Customers')

        for row in results:
            print(u'CustomerId: {}, Email: {}, FirstName: {}, LastName: {}'.format(*row))

# [START spanner_update_data]
def update_data(instance_id, database_id):
    """Updates sample data in the database.

    This updates the `MarketingBudget` column which must be created before
    running this sample. You can add the column by running the `add_column`
    sample or by running this DDL statement against your database:

        ALTER TABLE Albums ADD COLUMN MarketingBudget INT64

    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.update(
            table='Albums',
            columns=(
                'SingerId', 'AlbumId', 'MarketingBudget'),
            values=[
                (1, 1, 100000),
                (2, 2, 500000)])

    print('Updated data.')
# [END spanner_update_data]

main()