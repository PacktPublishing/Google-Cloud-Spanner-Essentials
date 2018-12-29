from google.cloud import spanner

instance_id = 'test-instance' #id of the spanner instance
database_id = 'example-db' #id of the spanner Database

def main():
    test(instance_id,database_id)

def test(instance_id, database_id):
	#instantiate a client 
    spanner_client = spanner.Client(project='constant-wonder-224208')   
    #Getting cloud spanner instance via id
    instance = spanner_client.instance(instance_id) 
    #getting cloud spanner db by db id
    database = instance.database(database_id) 
    #taking the DB snapshot to execute queries
    with database.snapshot() as snapshot: 
        #Execution of simple query using execute_sql method of snapshot object
        results = snapshot.execute_sql('SELECT 1') 

    for row in results:
        print(row) #result should print 1

main()