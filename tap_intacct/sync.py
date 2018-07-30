from singer import metadata, utils, Transformer

import singer
import singer_encodings.csv as csv
from tap_intacct import s3

LOGGER = singer.get_logger()

def sync_stream(config, state, stream):
    table_name = stream['tap_stream_id']
    modified_since = utils.strptime_with_tz(singer.get_bookmark(state, table_name, 'modified_since') or
                                            config['start_date'])

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    s3_files = s3.get_input_files_for_table(config, table_name, modified_since)

    LOGGER.info('Found %s files to be synced.', len(s3_files))

    records_streamed = 0
    if not s3_files:
        return records_streamed

    for s3_file in s3_files:
        records_streamed += sync_table_file(config, s3_file['key'], stream)

        state = singer.write_bookmark(state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed

def sync_table_file(config, s3_path, stream):
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = stream['tap_stream_id']

    s3_file_handle = s3.get_file_handle(config, s3_path)
    iterator = csv.get_row_iterator(s3_file_handle._raw_stream)

    records_synced = 0

    for row in iterator:
        custom_columns = {
            '_sdc_source_bucket': bucket,
            '_sdc_source_file': s3_path,

            # index zero, +1 for header row
            '_sdc_source_lineno': records_synced + 2
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, to_write)
        records_synced += 1

    return records_synced
