from google.cloud import spanner

instance_id = 'test-instance'
database_id = 'singers-db'


def main():
    #read_data(instance_id, database_id)
    read_stale_data(instance_id, database_id)
  
def read_data(instance_id, database_id):
    """Reads sample data from the database."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        keyset = spanner.KeySet(all_=True)
        results = snapshot.read(
            table='Albums',
            columns=('SingerId', 'AlbumId', 'AlbumTitle',),
            keyset=keyset,)

        for row in results:
            print(u'SingerId: {}, AlbumId: {}, AlbumTitle: {}'.format(*row))

def read_stale_data(instance_id, database_id):
   
    import datetime

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    staleness = datetime.timedelta(seconds=200)

    with database.snapshot(exact_staleness=staleness) as snapshot:
        keyset = spanner.KeySet(all_=True)
        results = snapshot.read(
            table='Albums',
            columns=('SingerId', 'AlbumId', 'MarketingBudget',),
            keyset=keyset)

        for row in results:
            print(u'SingerId: {}, AlbumId: {}, MarketingBudget: {}'.format(
                *row))
            #To use a bounded-staleness timestamp bound, specify a max_staleness value instead of the exact_staleness value.


main()