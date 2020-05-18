"""
A class that simulates a data governance server, which logs the ids of
records observed in a data processing pipeline.
"""
import sys, time, argparse, json, requests
import ray

@ray.remote
class DataGovernanceSystem:
    """
    Logs records by id. Example application: data governance.
    """
    def __init__(self, name = 'DataGovernanceSystem'):
        self.name = name
        self.ids = []
        self.start_time = time.time()

    def log(self, id_to_log):
        """
        Log record ids that have been processed.
        Returns the new count.
        """
        self.ids.append(id_to_log)
        return self.get_count()

    def get_ids(self):
        """Return the ids logged. Don't call this if the list is long!"""
        return self.ids

    def get_count(self):
        """Return the count of ids logged."""
        return len(self.ids)

    def reset(self):
        """Forget all ids that have been logged."""
        self.ids = []

    def get_start_time(self):
        return self.start_time

    def get_up_time(self):
        return time.time() - self.start_time

class Record:
    """Wrap a "datum"; provides an id to be logged and a "black box" for the data."""
    def __init__(self, record_id, data):
        self.record_id = record_id
        self.data = data
    def __str__(self):
        return f'Record(record_id={self.record_id},data={self.data})'

def do_test(dgs):
    """Try out the DGS."""
    n=10
    records = [Record(i, f'data for record {i}') for i in range(n)] # sample "records"

    print(f'Logging {n} records...')
    for record in records:
        dgs.log.remote(record.record_id)

    count = ray.get(dgs.get_count.remote())
    print(f'count: {count}')

    ids = ray.get(dgs.get_ids.remote())
    print(f'ids: {ids}')

    up_time = ray.get(dgs.get_up_time.remote())
    print(f'up time: {up_time}')

def main():
    """
    If you run this as a script, it will effectively disappear when the script finishes,
    even if you specify --auto or --address. Useful for testing, however.
    """
    parser = argparse.ArgumentParser(description="Data Governance example")
    parser.add_argument('--local', help="Run standalone, for local testing. Use this, --auto, or --address.",
                        action='store_true')
    parser.add_argument('--auto', help="Join a cluster already running. Use this, --local, or --address.",
                        action='store_true')
    parser.add_argument('--address', metavar='str', type=str, default=None, nargs='?',
                        help='Join a Ray cluster at a specific address. Use this, --auto, or --local.')
    args = parser.parse_args()

    if args.local and not args.auto and not args.address:
        ray.init()
    elif args.auto and not args.local and not args.address:
        ray.init(address='auto')
    elif args.address and not args.local and not args.auto:
        ray.init(address=args.address)
    else:
        print('ERROR: must specify one of --local, --auto, or --address ADDRESS')
        sys.exit(1)

    dgs = DataGovernanceSystem.remote(name = 'DataGovernanceSystem')
    do_test(dgs)

if __name__ == '__main__':
    main()
