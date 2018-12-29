from google.cloud import spanner

instance_id = 'test-instance'
database_id = 'sales-db'


def main():
    #add_interleaved_index(instance_id, database_id)

    #add_storing_index(instance_id, database_id)
    #query_data_with_index(instance_id, database_id)
    read_data_with_index(instance_id, database_id)
    read_data_with_storing_index(instance_id, database_id)
   

def add_index(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    operation = database.update_ddl([
        'CREATE INDEX CustomersByFirstLastName ON Customers(FirstName,LastName)'])

    print('Waiting for operation to complete...')
    operation.result()

    print('Added the CustomersByFirstLastName index.')

def add_interleaved_index(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    operation = database.update_ddl([
        
        'CREATE INDEX ProductsByCustomerId ON LineItems(CustomerId, ProductId),INTERLEAVE IN Customers'])

    print('Waiting for operation to complete...')
    operation.result()

    print('Added the LineitemsByCustomerProduct  index.')

def add_storing_index(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    operation = database.update_ddl([
        'CREATE INDEX CustomersByFirstLastNameStored ON Customers(FirstName,LastName)'
        'STORING (Email)'])

    print('Waiting for operation to complete...')
    operation.result()

    print('Added the CustomersByFirstLastName storing index.')


def query_data_with_index(instance_id, database_id):

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)



    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT CustomerId, FirstName, Email "
            "FROM Customers@{FORCE_INDEX=CustomersByFirstLastName} "
            "WHERE FirstName > 'Asma'")
    
        for row in results:
            print(
                u'CustomerId: {}, FirstName: {}, '
                'Email: {}'.format(*row))


def read_data_with_index(instance_id, database_id):
  
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        keyset = spanner.KeySet(all_=True)
        results = snapshot.read(
            table='Customers',
            columns=('CustomerId', 'FirstName', 'LastName'),
            keyset=keyset,
            index='CustomersByFirstLastName')

        for row in results:
            print('CustomerId: {}, FirstName: {}, LastName: {}'.format(*row))


def read_data_with_storing_index(instance_id, database_id):
   
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        keyset = spanner.KeySet(all_=True)
        results = snapshot.read(
            table='Customers',
            columns=('CustomerId', 'FirstName', 'Email'),
            keyset=keyset,
            index='CustomersByFirstLastNameStored')

        for row in results:
            print(
                u'CustomerId: {}, FirstName: {}, '
                'Email: {}'.format(*row))

main()