from google.cloud import spanner

instance_id = 'test-instance'
database_id = 'sales-db'
database_id2 = 'singers-db'


def main():
    insert_data(instance_id, database_id2)
    update_data(instance_id, database_id2)
    insert_data_with_dml(instance_id, database_id2)
    insert_with_dml(instance_id, database_id2)
    update_data_with_dml(instance_id, database_id2)
    add_timestamp_column(instance_id, database_id2)
    update_data_with_dml_timestamp(instance_id, database_id2)


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
def insert_data(instance_id, database_id): 
 
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table='Singers',
            columns=('SingerId', 'FirstName', 'LastName',),
            values=[
                (12, u'Smith', u'Richards'),
                (13, u'Donna', u'Lea'),
                (14, u'Cold', u'stan'),
                (15, u'Haley', u'Martin'),
                (16, u'Sam', u'Ness')])

        batch.insert(
            table='Albums',
            columns=('SingerId', 'AlbumId', 'AlbumTitle',),
            values=[
                (12, 1, u'Storm Nights'),
                (12, 2, u'Rolling Moss')])

    print('Inserted data.') #Mutations #With Mutations
def delete_data(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    singers_to_delete = spanner.KeySet(
        keys=[[1], [2], [3], [4], [5]])
    albums_to_delete = spanner.KeySet(
        keys=[[1, 1], [1, 2], [2, 1], [2, 2], [2, 3]])

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
                (1, 1, 100000),
                (2, 2, 500000)])

    print('Updated data.')

def update_data_with_timestamp(instance_id, database_id):
    
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(database_id)

    with database.batch() as batch:
        batch.update(
            table='Albums',
            columns=(
                'SingerId', 'AlbumId', 'MarketingBudget', 'LastUpdateTime'),
            values=[
                (1, 1, 1000000, spanner.COMMIT_TIMESTAMP),
                (2, 2, 750000, spanner.COMMIT_TIMESTAMP)])

    print('Updated data.')
def insert_data_with_dml(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def insert_singers(transaction):
        row_ct = transaction.execute_update(
            "INSERT Singers (SingerId, FirstName, LastName) "
            " VALUES (6, 'Virginia', 'Watson')"
        )

        print("{} record(s) inserted.".format(row_ct))

    database.run_in_transaction(insert_singers)
    # [END spanner_dml_standard_insert]
def insert_with_dml(instance_id, database_id):
    """Inserts data with a DML statement into the database. """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def insert_singers(transaction):
        row_ct = transaction.execute_update(
            "INSERT Singers (SingerId, FirstName, LastName) VALUES "
            "(7, 'Melissa', 'Garcia'), "
            "(8, 'Russell', 'Morales'), "
            "(9, 'Jacqueline', 'Long'), "
            "(10, 'Dylan', 'Shaw')"
        )
        print("{} record(s) inserted.".format(row_ct))

    database.run_in_transaction(insert_singers)
    # [END spanner_dml_getting_started_insert]

def update_data_with_dml(instance_id, database_id):
 
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def update_albums(transaction):
        row_ct = transaction.execute_update(
            "UPDATE Albums "
            "SET MarketingBudget = MarketingBudget * 2 "
            "WHERE SingerId = 1 and AlbumId = 1"
        )

        print("{} record(s) updated.".format(row_ct))

    database.run_in_transaction(update_albums)
    
def delete_data_with_dml(instance_id, database_id):

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def delete_singers(transaction):
        row_ct = transaction.execute_update(
            "DELETE Singers WHERE FirstName = 'Alice'"
        )

        print("{} record(s) deleted.".format(row_ct))

    database.run_in_transaction(delete_singers)
    # [END spanner_dml_standard_delete]

def add_timestamp_column(instance_id, database_id):
    """ Adds a new TIMESTAMP column to the Albums table in the example database.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(database_id)

    operation = database.update_ddl([
        'ALTER TABLE Albums ADD COLUMN LastUpdateTime TIMESTAMP '
        'OPTIONS(allow_commit_timestamp=true)'])

    print('Waiting for operation to complete...')
    operation.result()

    print('Altered table "Albums" on database {} on instance {}.'.format(
        database_id, instance_id))

def update_data_with_dml_timestamp(instance_id, database_id):
    """Updates data with Timestamp from the database using a DML statement. """
    # [START spanner_dml_standard_update_with_timestamp]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def update_albums(transaction):
        row_ct = transaction.execute_update(
            "UPDATE Albums "
            "SET LastUpdateTime = PENDING_COMMIT_TIMESTAMP() "
            "WHERE SingerId = 1"
        )

        print("{} record(s) updated.".format(row_ct))

    database.run_in_transaction(update_albums)
    # [END spanner_dml_standard_update_with_timestamp]


def dml_write_read_transaction(instance_id, database_id):
    """First inserts data then reads it from within a transaction using DML."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def write_then_read(transaction):
        # Insert record.
        row_ct = transaction.execute_update(
            "INSERT Singers (SingerId, FirstName, LastName) "
            " VALUES (11, 'Timothy', 'Campbell')"
        )
        print("{} record(s) inserted.".format(row_ct))

        # Read newly inserted record.
        results = transaction.execute_sql(
            "SELECT FirstName, LastName FROM Singers WHERE SingerId = 11"
        )
        for result in results:
            print("FirstName: {}, LastName: {}".format(*result))

    database.run_in_transaction(write_then_read)
    
def write_with_dml_transaction(instance_id, database_id):
    """ Transfers a marketing budget from one album to another. """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def transfer_budget(transaction):
        # Transfer marketing budget from one album to another. Performed in a
        # single transaction to ensure that the transfer is atomic.
        first_album_result = transaction.execute_sql(
            "SELECT MarketingBudget from Albums "
            "WHERE SingerId = 1 and AlbumId = 1"
        )
        first_album_row = list(first_album_result)[0]
        first_album_budget = first_album_row[0]

        transfer_amount = 300000

        # Transaction will only be committed if this condition still holds at
        # the time of commit. Otherwise it will be aborted and the callable
        # will be rerun by the client library
        if first_album_budget >= transfer_amount:
            second_album_result = transaction.execute_sql(
                "SELECT MarketingBudget from Albums "
                "WHERE SingerId = 2 and AlbumId = 2"
            )
            second_album_row = list(second_album_result)[0]
            second_album_budget = second_album_row[0]

            first_album_budget -= transfer_amount
            second_album_budget += transfer_amount

            # Update first album
            transaction.execute_update(
                "UPDATE Albums "
                "SET MarketingBudget = @AlbumBudget "
                "WHERE SingerId = 1 and AlbumId = 1",
                params={"AlbumBudget": first_album_budget},
                param_types={"AlbumBudget": spanner.param_types.INT64}
            )

            # Update second album
            transaction.execute_update(
                "UPDATE Albums "
                "SET MarketingBudget = @AlbumBudget "
                "WHERE SingerId = 2 and AlbumId = 2",
                params={"AlbumBudget": second_album_budget},
                param_types={"AlbumBudget": spanner.param_types.INT64}
            )

            print("Transferred {} from Album1's budget to Album2's".format(
                    transfer_amount))

    database.run_in_transaction(transfer_budget)
    # [END spanner_dml_getting_started_update]

def update_data_with_partitioned_dml(instance_id, database_id):
   

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    row_ct = database.execute_partitioned_dml(
        "UPDATE Albums SET MarketingBudget = 100000 WHERE SingerId > 1"
    )

    print("{} records updated.".format(row_ct))
    
def delete_data_with_partitioned_dml(instance_id, database_id):
  
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    row_ct = database.execute_partitioned_dml(
        "DELETE Singers WHERE SingerId > 10"
    )

    print("{} record(s) deleted.".format(row_ct))
   
def update_data_with_dml_struct(instance_id, database_id):
    """Updates data with a DML statement and STRUCT parameters. """
    # [START spanner_dml_structs]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    record_type = param_types.Struct([
        param_types.StructField('FirstName', param_types.STRING),
        param_types.StructField('LastName', param_types.STRING)
    ])
    record_value = ('Timothy', 'Campbell')

    def write_with_struct(transaction):
        row_ct = transaction.execute_update(
            "UPDATE Singers SET LastName = 'Grant' "
            "WHERE STRUCT<FirstName STRING, LastName STRING>"
            "(FirstName, LastName) = @name",
            params={'name': record_value},
            param_types={'name': record_type}
        )
        print("{} record(s) updated.".format(row_ct))

    database.run_in_transaction(write_with_struct)
    # [END spanner_dml_structs]

main()