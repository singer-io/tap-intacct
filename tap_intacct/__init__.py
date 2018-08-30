import json
import sys
import singer

from singer import metadata
from tap_intacct.discover import discover_streams
from tap_intacct.sync import sync_stream

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "company_id", "account_id", "external_id", "role_name"]

def setup_aws_client(config):
    client = boto3.client('sts')
    role_arn = "arn:aws:iam::{}:role/{}".format(config['account_id'], config['role_name'])

    role = client.assume_role(RoleArn=role_arn, ExternalId=config['external_id'], RoleSessionName='TapIntacct')
    boto3.setup_default_session(aws_access_key_id=role['Credentials']['AccessKeyId'], aws_secret_access_key=role['Credentials']['SecretAccessKey'], aws_session_token=role['Credentials']['SessionToken'])


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties') or []
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.properties, args.state)


if __name__ == '__main__':
    main()
