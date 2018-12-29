from google.cloud import spanner

instance_id = 'test-instance'
database_id = 'sales-db'
database_id2 = 'singers-db'


def main():
    insert_data(instance_id, database_id2)
    
    update_data(instance_id, database_id2)
    delete_data(instance_id, database_id2)
  


# [START spanner_add_column]
def add_column(instance_id, database_id):
    """Adds a new column to the Albums table in the example database."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id2)
   
    operation = database.update_ddl([
        'ALTER TABLE Albums ADD COLUMN MarketingBudget INT64'])
    #database.list()
    print('Waiting for operation to complete...')
    print(operation.result())
    
    print('Added the MarketingBudget column.')
# [END spanner_add_column]
def add_index(instance_id, database_id):
    """Adds a simple index to the example database."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    operation = database.update_ddl([
        'CREATE INDEX AlbumsByAlbumTitle ON Albums(AlbumTitle)'])

    print('Waiting for operation to complete...')
    operation.result()

    print('Added the AlbumsByAlbumTitle index.')

def add_storing_index(instance_id, database_id):
    """Adds an storing index to the example database."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    operation = database.update_ddl([
        'CREATE INDEX AlbumsByAlbumTitle2 ON Albums(AlbumTitle)'
        'STORING (MarketingBudget)'])

    print('Waiting for operation to complete...')
    operation.result()

    print('Added the AlbumsByAlbumTitle2 index.')
def create_database(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(database_id, ddl_statements=[
        """CREATE TABLE Customers (
            CustomerId     INT64 NOT NULL,
            FirstName    STRING(1024),
            LastName     STRING(1024),
            Email   BYTES(MAX)
        ) PRIMARY KEY (CustomerId)""",
        """CREATE TABLE Invoices (
            CustomerId     INT64 NOT NULL,
            InvoiceId      INT64 NOT NULL,
            TotalAmount    INT64

        ) PRIMARY KEY (CustomerId, InvoiceId),
        INTERLEAVE IN PARENT Customers ON DELETE CASCADE""",
        """CREATE TABLE LineItems (
            
            CustomerId    INT64 NOT NULL,
            InvoiceId     INT64 NOT NULL,
            LineitemId    INT64 NOT NULL,
            ProductId     INT64, 
            Quantity      INT64

        ) PRIMARY KEY (CustomerId,InvoiceId,LineitemId),
        INTERLEAVE IN PARENT Invoices ON DELETE CASCADE""",
        """CREATE TABLE Products (
            ProductId     INT64 NOT NULL,
            Item          STRING(1024),
            ProductDesc   STRING(MAX),
            ProductPrice  INT64
        ) PRIMARY KEY (ProductId)""",
        
    ])

    operation = database.create()


    print('Waiting for completion')
    operation.result()

    print('Created database {} on instance {}'.format(
        database_id, instance_id))
def create_database2(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(database_id, ddl_statements=[
        """CREATE TABLE Singers (
            SingerId     INT64 NOT NULL,
            FirstName    STRING(1024),
            LastName     STRING(1024),
            SingerInfo   BYTES(MAX)
        ) PRIMARY KEY (SingerId)""",
        """CREATE TABLE Albums (
            SingerId     INT64 NOT NULL,
            AlbumId      INT64 NOT NULL,
            AlbumTitle   STRING(MAX)
        ) PRIMARY KEY (SingerId, AlbumId),
        INTERLEAVE IN PARENT Singers ON DELETE CASCADE"""
    ])

    operation = database.create()

    print('Waiting for completion')
    operation.result()

    print('Created database {} on instance {}'.format(
        database_id, instance_id))   
def insert_data(instance_id, database_id): 
 
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table='Singers',
            columns=('SingerId', 'FirstName', 'LastName',),
            values=[
                (17, u'Smith', u'Richards'),
                (18, u'Donna', u'Lea'),
                (19, u'Cold', u'stan')])

        batch.insert(
            table='Albums',
            columns=('SingerId', 'AlbumId', 'AlbumTitle',),
            values=[
                (17, 1, u'Storm Nights'),
                (17, 2, u'Rolling Moss')])

    print('Inserted data.') #Mutations #With Mutations
def delete_data(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    singers_to_delete = spanner.KeySet(
        keys=[[11], [12], [13], [14], [15]])
    albums_to_delete = spanner.KeySet(
        keys=[[12, 1], [12, 2]])

    with database.batch() as batch:
        batch.delete('Albums', albums_to_delete)
        batch.delete('Singers', singers_to_delete)

    print('Deleted data.')
def read_write_transaction(instance_id, database_id):
  
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def update_albums(transaction):
        # Read the second album budget.
        second_album_keyset = spanner.KeySet(keys=[(2, 2)])
        second_album_result = transaction.read(
            table='Albums', columns=('MarketingBudget',),
            keyset=second_album_keyset, limit=1)
        second_album_row = list(second_album_result)[0]
        second_album_budget = second_album_row[0]

        transfer_amount = 200000

        if second_album_budget < 300000:
            # Raising an exception will automatically roll back the
            # transaction.
            raise ValueError(
                'The second album doesn\'t have enough funds to transfer')

        # Read the first album's budget.
        first_album_keyset = spanner.KeySet(keys=[(1, 1)])
        first_album_result = transaction.read(
            table='Albums', columns=('MarketingBudget',),
            keyset=first_album_keyset, limit=1)
        first_album_row = list(first_album_result)[0]
        first_album_budget = first_album_row[0]

        # Update the budgets.
        second_album_budget -= transfer_amount
        first_album_budget += transfer_amount
        print(
            'Setting first album\'s budget to {} and the second album\'s '
            'budget to {}.'.format(
                first_album_budget, second_album_budget))

        # Update the rows.
        transaction.update(
            table='Albums',
            columns=(
                'SingerId', 'AlbumId', 'MarketingBudget'),
            values=[
                (1, 1, first_album_budget),
                (2, 2, second_album_budget)])

    database.run_in_transaction(update_albums)

    print('Transaction complete.')
def update_data(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.update(
            table='Albums',
            columns=(
                'SingerId', 'AlbumId', 'MarketingBudget'),
            values=[
                (12, 1, 100000),
                (12, 2, 500000)])

    print('Updated data.')


main()