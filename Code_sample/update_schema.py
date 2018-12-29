from google.cloud import spanner

instance_id= 'test-instance'
database_id = 'singers-db'


def main():
    add_column(instance_id, database_id)

def add_column(instance_id, database_id):
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
   
    operation = database.update_ddl([
        'ALTER TABLE Albums ADD COLUMN MarketingBudget INT64'])

    print('Waiting for operation to complete...')
    print(operation.result())
    
    print('Added the MarketingBudget column.')

main()