"""
A Ray Serve implementation of a simulated data governance server that logs the ids of
records observed in a data processing pipeline.
"""
import sys, time, argparse, json, requests
import ray
from ray import serve
from ray.util import named_actors
from data_governance_system import DataGovernanceSystem, Record

def init_service(dgs_server_name, serve_port):
    """Initialize the server."""

    def handle_log(request):
        """Handler for logging ids."""
        gov = named_actors.get_actor(dgs_server_name)
        record_id = request.args.get('id', None)
        if not record_id:
            raise Exception(f'Invalid "log" request, missing id. Request = {request}')
        else:
            gov.log.remote(record_id) # fire and forget
            return json.dumps({'message': f'sent async log request for {record_id}'})

    def handle_get_ids():
        """
        Returns the logged ids, which could be huge!
        NOTE: This method makes a blocking call to the DataGovernanceSystem actor.
        """
        gov = named_actors.get_actor(dgs_server_name)
        ids = ray.get(gov.get_ids.remote())
        return json.dumps({'ids': ids})

    def handle_get_count():
        """
        Returns the count of logged ids.
        NOTE: This method makes a blocking call to the DataGovernanceSystem actor.
        """
        gov = named_actors.get_actor(dgs_server_name)
        count = ray.get(gov.get_count.remote())
        return json.dumps({'count': count})

    def handle_reset():
        """
        Handle a reset request.
        NOTE: This method makes a "fire and forget", non-blocking call to the DataGovernanceSystem actor.
        """
        gov = named_actors.get_actor(dgs_server_name)
        gov.reset.remote()
        return json.dumps({'message': 'async reset invoked.'})


    def handle_start_time():
        """
        Handle a start time request.
        NOTE: This method makes a blocking call to the DataGovernanceSystem actor.
        """
        gov = named_actors.get_actor(dgs_server_name)
        start_time = ray.get(gov.get_start_time.remote())
        return json.dumps({'start_time': start_time})

    def handle_up_time():
        """
        Handle a up time request.
        NOTE: This method makes a blocking call to the DataGovernanceSystem actor.
        """
        gov = named_actors.get_actor(dgs_server_name)
        up_time = ray.get(gov.get_up_time.remote())
        return json.dumps({'up_time': up_time})

    def handle_exit():
        """Handle an exit (quit) request."""
        sys.exit()

    gov = DataGovernanceSystem.remote()
    named_actors.register_actor(dgs_server_name, gov)

    serve.init(http_port=serve_port)

    serve.create_endpoint('log', '/log', methods=['PUT'])
    serve.create_backend('log', handle_log)
    serve.set_traffic('log', {'log': 1.0})

    serve.create_endpoint('ids', '/ids', methods=['GET'])
    serve.create_backend('ids', handle_get_ids)
    serve.set_traffic('ids', {'ids': 1.0})

    serve.create_endpoint('count', '/count', methods=['GET'])
    serve.create_backend('count', handle_get_count)
    serve.set_traffic('count', {'count': 1.0})

    serve.create_endpoint('reset', '/reset', methods=['PUT', 'GET'])
    serve.create_backend('reset', handle_reset)
    serve.set_traffic('reset', {'reset': 1.0})

    serve.create_endpoint('start_time', '/start_time', methods=['PUT', 'GET', 'POST'])
    serve.create_backend('start_time', handle_start_time)
    serve.set_traffic('start_time', {'start_time': 1.0})

    serve.create_endpoint('up_time', '/up_time', methods=['PUT', 'GET', 'POST'])
    serve.create_backend('up_time', handle_up_time)
    serve.set_traffic('up_time', {'up_time': 1.0})

    serve.create_endpoint('exit', '/exit', methods=['PUT', 'GET', 'POST'])
    serve.create_backend('exit', handle_exit)
    serve.set_traffic('exit', {'exit': 1.0})
    return dgs_server_name


def do_test(port, timeout):
    """Try out the server."""
    records = [Record(i, f'data for record {i}') for i in range(10)] # 10 sample "records"

    address = f'http://127.0.0.1:{port}'
    print(f'Putting 10 records... to {address}')
    for record in records:
        response = requests.put(f'{address}/log?id={record.record_id}', timeout=timeout)
        print(f'log response = {response.json()}')

    count = requests.get(f'{address}/count', timeout=timeout)
    print(f'count reponse = {count.json()}')

    ids = requests.get(f'{address}/ids', timeout=timeout)
    print(f'ids reponse = {ids.json()}')


def main():
    """
    If you run this as a script, it will effectively disappear when the script finishes,
    even if you specify --auto or --address. Useful for testing, however.
    """
    parser = argparse.ArgumentParser(description="Ray Serve Demo")
    parser.add_argument('--local', help="Run locally. Use this, --auto, or --address.",
                        action='store_true')
    parser.add_argument('--auto', help="Join a cluster already running. Use this, --local, or --address.",
                        action='store_true')
    parser.add_argument('--address', metavar='str', type=str, default=None, nargs='?',
                        help='Join a Ray cluster at a specific address. Use this, --auto, or --local.')
    parser.add_argument('--port', metavar='N', type=int, default=8000, nargs='?',
                        help='The port for Serve to listen for requests.')
    parser.add_argument('--timeout', metavar='M.N', type=float, default=1.0, nargs='?',
                        help='Timeout (seconds) for web requests.')
    parser.add_argument('--server-name', metavar='name', type=str, default='dgs', nargs='?',
                        help='Name of the detached actor for the data governance server.')
    args = parser.parse_args()

    if args.local and not args.auto and not args.address:
        pass
    elif args.auto and not args.local and not args.address:
        ray.init(address='auto')
    elif args.address and not args.local and not args.auto:
        ray.init(address=args.address)
    else:
        print('ERROR: must specify one of --local, --auto, or --address ADDRESS')
        sys.exit(1)

    init_service(args.server_name, args.port)
    do_test(args.port, args.timeout)

if __name__ == '__main__':
    main()
