import elasticsearch
import jinja2

from typing import List, Dict, Optional, Union, Any, Tuple, Set

import re
import io
import csv
import time
import concurrent
import jsonpath_ng
import singer_sdk.io_base
import pydash
import difflib
import pandas as pd
import numpy as np
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
from difflib import SequenceMatcher
from itertools import zip_longest
from functools import lru_cache

import datetime
import logging

from target_elasticsearch.common import (
    INDEX_FORMAT,
    SCHEME,
    HOST,
    PORT,
    USERNAME,
    PASSWORD,
    BEARER_TOKEN,
    API_KEY_ID,
    API_KEY,
    ENCODED_API_KEY,
    SSL_CA_FILE,
    INDEX_TEMPLATE_FIELDS,
    ELASTIC_YEARLY_FORMAT,
    ELASTIC_MONTHLY_FORMAT,
    ELASTIC_DAILY_FORMAT,
    METADATA_FIELDS,
    NAME,
    PREFERRED_PKEY,
    CHECK_DIFF,
    STREAM_NAME,
    EVENT_TIME_KEY,
    IGNORED_FIELDS,
    DEFAULT_IGNORED_FIELDS,
    SPECIFIC_DIFF_PROCESS_CSV,
    SPECIFIC_DIFF_PROCESS_TEXT,
    SPECIFIC_DIFF_PROCESS_TEXT_LINE,
    SPECIFIC_DIFF_PROCESS_DICT,
    SPECIFIC_DIFF_PROCESS_FLAT,
    SPECIFIC_DIFF_PROCESS,
    SPECIFIC_DIFF_PROCESS_DATA_FIELD,
    SPECIFIC_DIFF_PROCESS_FILTER_FIELD,
    SPECIFIC_DIFF_PROCESS_FILTER_VALUE,
    DIFF_SUFFIX,
    to_daily,
    to_monthly,
    to_yearly,
)


def template_index(stream_name: str, index_format: str, schemas: Dict) -> str:
    """
    _index templates the input index config to be used for elasticsearch indexing
    currently it operates using current time as index.
    this may not be optimal and additional support can be added to parse @timestamp out and use it in index
    templating depending on how your elastic instance is configured.
    @param stream_name:
    @param index_format:
    @param schemas:
    @return: str
    """
    today = datetime.date.today()
    arguments = {
        **{
            "stream_name": stream_name,
            "current_timestamp_daily": today.strftime(ELASTIC_DAILY_FORMAT),
            "current_timestamp_monthly": today.strftime(ELASTIC_MONTHLY_FORMAT),
            "current_timestamp_yearly": today.strftime(ELASTIC_YEARLY_FORMAT),
            "to_daily": to_daily,
            "to_monthly": to_monthly,
            "to_yearly": to_yearly,
        },
        **schemas,
    }
    environment = jinja2.Environment()
    template = environment.from_string(index_format)
    return template.render(**arguments).replace("_", "-")


def build_fields(
    stream_name: str,
    mapping: Dict,
    record: Dict[str, Union[str, Dict[str, str], int]],
    logger: singer_sdk.io_base.logger,
) -> Dict:
    """
    build_fields parses records for supplied mapping to be used later in index templating and ecs metadata field formulation
    @param logger:
    @param stream_name:
    @param mapping: dict
    @param record:  str
    @return: dict
    """
    schemas = {}
    if stream_name in mapping:
        logger.debug(INDEX_TEMPLATE_FIELDS, ": ", mapping[stream_name])
        for k, v in mapping[stream_name].items():
            match = jsonpath_ng.parse(v).find(record)
            if len(match) == 0:
                logger.warning(
                    f"schema key {k} with json path {v} could not be found in record: {record}"
                )
                schemas[k] = v
            else:
                if len(match) < 1:
                    logger.warning(
                        f"schema key {k} with json path {v} has multiple associated fields, may cause side effects"
                    )
                schemas[k] = match[0].value
    return schemas


class ElasticSink(BatchSink):
    """ElasticSink target sink class."""

    # From 1000 to 200: we tend to insert big records, so don't hit as hard
    max_size = 200  # Max records to write in one batch

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ):
        super().__init__(target, stream_name, schema, key_properties)
        self.client = self._authenticated_client()
        # Overload Elasticsearch's logging level
        logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)


    def build_request_body_and_distinct_indices(
        self, records: List[Dict[str, Union[str, Dict[str, str], int]]]
    ) -> Tuple[List[Dict[Union[str, Any], Union[str, Any]]], Set[str]]:
        """
        build_request_body_and_distinct_indices builds the bulk request body
        and collects all distinct indices that will be used to create indices before bulk upload.
        @param records:
        @return:
        """
        updated_records = []
        index_mapping = {}
        metadata_fields = {}
        distinct_indices = set()
        if INDEX_TEMPLATE_FIELDS in self.config:
            index_mapping = self.config[INDEX_TEMPLATE_FIELDS]
        if METADATA_FIELDS in self.config:
            metadata_fields = self.config[METADATA_FIELDS]
        # There may be multiple diff configurations for a given stream name (eg. depending on the value
        # of a field, apply a specific filter)
        diff_enabled = [elem for elem in self.config[CHECK_DIFF]
                        if re.search(elem[STREAM_NAME], self.stream_name)]
        self.logger.info(
            f"Diff enabled for stream {self.stream_name}:: {len(diff_enabled) > 0}")
        for r in records:
            index = template_index(
                self.stream_name,
                self.config[INDEX_FORMAT],
                build_fields(self.stream_name, index_mapping, r, self.logger),
            )
            distinct_indices.add(index)

            doc_id = self.build_doc_id(self.stream_name, r)
            if doc_id != "":
                # Upsert logic:
                # ctx.op == create => If the document does not exist, insert r including _sdc_sequence
                # Else, if the document exists, update with all fields from r except _sdc_sequence
                updated_records.append({
                    "_op_type": "update",
                    "_index": index,
                    "_id": doc_id,
                    "scripted_upsert": True,
                    "script": {
                        "source": """
                        if (ctx.op == 'create') {
                            ctx._source.putAll(params.r);
                        } else {
                            for (entry in params.r.entrySet()) {
                                if (!entry.getKey().equals('_sdc_sequence')) { // Skip _sdc_sequence field
                                    ctx._source.put(
                                        entry.getKey(), entry.getValue());
                                }
                            }
                        }
                        """,
                        "lang": "painless",
                        "params": {
                            "r": {
                                **r,
                                **build_fields(self.stream_name, metadata_fields, r, self.logger)
                            }
                        }
                    },
                    "upsert": {}  # Empty document for upsert; actual content is managed by the script
                })
            else:
                # Default insertion
                updated_records.append(
                    {
                        **{"_op_type": "index", "_index": index, "_source": r},
                        **build_fields(self.stream_name, metadata_fields, r, self.logger),
                    }
                )

        if len(diff_enabled) > 0 and len(records) > 0:
            index = template_index(
                self.stream_name,
                self.config[INDEX_FORMAT],
                build_fields(self.stream_name, index_mapping, r, self.logger),
            )
            distinct_indices.add(index+DIFF_SUFFIX)
            # Diff processing is something which can take a long time, as queries need to be made
            # -> Parallelize these checks
            self.logger.info(
                f"Generate diff records based on the {len(records)} new records")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(self.process_main_records_make_diffs, record, index, diff_enabled, metadata_fields)
                           for record in records]
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result is not None:
                        updated_records.append(result)

        return updated_records, distinct_indices

    def create_indices(self, indices: Set[str]) -> None:
        """
        create_indices creates elastic indices using cluster defaults
        @param indices: set
        """
        for index in indices:
            try:
                settings = {
                    "index": {
                        "mapping": {
                            # Do not raise an error if the schema is not perfect - eg. empty string for a null date
                            "ignore_malformed": True,
                            "total_fields": {
                                # Default value is 1000, but some documents may get very large
                                "limit": 2000
                            }
                        }
                    }
                }
                mapping = None
                if DIFF_SUFFIX in index:
                    mapping = {
                        "dynamic": "false",
                        "properties": {
                            "id": {"type": "keyword"},
                            "main_doc_id": {"type": "keyword"},
                            "event_ts": {"type": "date"},
                            "_sdc_batched_at": {"type": "date"},
                            "_sdc_extracted_at": {"type": "date"},
                            "_sdc_sequence": {"type": "long"},
                        }
                    }

                self.client.indices.create(
                    index=index,
                    settings=settings,
                    mappings=mapping
                )
                self.logger.info(f"created index {index}")
            except elasticsearch.exceptions.RequestError as e:
                if e.error == "resource_already_exists_exception":
                    self.logger.debug(
                        "index already created skipping creation")
                else:  # Other exception - raise it
                    raise e

    def build_body(
        self, records: List[Dict[str, Union[str, Dict[str, str], int]]]
    ) -> List[Dict[Union[str, Any], Union[str, Any]]]:
        """
        build_body constructs the bulk message body and creates all necessary indices if needed
        @param records: str
        @return: list[dict[Union[str, Any], Union[str, Any]]]
        """
        updated_records, distinct_indices = self.build_request_body_and_distinct_indices(
            records)
        self.create_indices(distinct_indices)
        return updated_records

    def _authenticated_client(self) -> elasticsearch.Elasticsearch:
        """
        _authenticated_client generates a newly authenticated elasticsearch client
        attempting to support all auth permutations and ssl concerns
        https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html
        @return: elasticsearch.Elasticsearch
        """
        config = {}
        scheme = self.config[SCHEME]
        if SSL_CA_FILE in self.config:
            scheme = "https"
            config["ca_certs"] = self.config[SSL_CA_FILE]

        config["hosts"] = [
            f"{scheme}://{self.config[HOST]}:{self.config[PORT]}"]

        if USERNAME in self.config and PASSWORD in self.config:
            config["basic_auth"] = (
                self.config[USERNAME], self.config[PASSWORD])
        elif API_KEY in self.config and API_KEY_ID in self.config:
            config["api_key"] = (self.config[API_KEY_ID], self.config[API_KEY])
        elif ENCODED_API_KEY in self.config:
            config["api_key"] = self.config[ENCODED_API_KEY]
        elif BEARER_TOKEN in self.config:
            config["bearer_auth"] = self.config[BEARER_TOKEN]
        else:
            self.logger.info("using default elastic search connection config")

        config["headers"] = {"user-agent": self._elasticsearch_user_agent()}

        return elasticsearch.Elasticsearch(**config)

    def write_output(self, records):
        """
        write_output creates indices, builds batch request body, and writing to elastic via bulk helper function
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#bulk-helpers
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        @param records:
        """
        records = self.build_body(records)
        self.logger.debug(records)

        MAX_RETRIES = 5
        RETRY_DELAY = 20

        for attempt in range(MAX_RETRIES):
            try:
                bulk(self.client, records)
                # Successful -> exit the loop
                break  
            except elasticsearch.helpers.BulkIndexError as e:
                self.logger.error(f"BulkIndexError on attempt {attempt + 1}: {e.errors}")
                raise  # Re-raise the BulkIndexError as it's not a connection issue
            except elasticsearch.exceptions.ConnectionTimeout as e:
                if attempt < MAX_RETRIES - 1:
                    self.logger.warning(f"ConnectionTimeout on attempt {attempt + 1}. Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    self.logger.error(f"ConnectionTimeout on final attempt {MAX_RETRIES}: {str(e)}")
                    raise
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                raise


    def process_batch(self, context: Dict[str, Any]) -> None:
        """
        process_batch handles batch records and overrides the default sink implementation
        @param context: dict
        """
        records = context["records"]
        self.write_output(records)
        self.tally_record_written(len(records))

    def clean_up(self) -> None:
        """
        clean_up closes the elasticsearch client
        """
        self.logger.debug(f"Cleaning up sink for {self.stream_name}")
        self.client.close()

    def _elasticsearch_user_agent(self) -> str:
        """
        Returns a user agent string for the elasticsearch client
        """
        return f"meltano-loader-elasticsearch/{PluginBase._get_package_version(NAME)}"

    def build_doc_id(self, stream_name: str, r: Dict[str, Union[str, Dict[str, str], int]]) -> str:
        # 1. Explicitly handled cases
        if stream_name in PREFERRED_PKEY:
            if False not in [x in r for x in PREFERRED_PKEY[stream_name]]:
                return "-".join([str(r[x]) for x in PREFERRED_PKEY[stream_name]])

        # 2. Best effort to avoid duplicates:
        # In descending priority, try to match a field which looks like a primary key
        id_fields = ["id", "ID", "Id", "accountId",
                     "sha", "hash", "node_id", "idx", "key", "ts", "name"]
        for id_field in id_fields:
            if id_field in r:
                return r[id_field]

        return ""

    def _validate_and_parse(self, record: dict) -> dict:
        try:
            self._validator.validate(record)
            self._parse_timestamps_in_record(
                record=record,
                schema=self.schema,
                treatment=self.datetime_error_treatment,
            )
        except Exception as e:
            self.logger.warning(f"Unexpected record raised an exception during validation: {e}. Record details:")
            self.logger.warning(record)
        return record

    def process_main_records_make_diffs(self, r, index, diff_enabled, metadata_fields):
        doc_id = self.build_doc_id(self.stream_name, r)
        if doc_id == "":
            return None
        # Multiple diff settings can be applied for a given stream, eg. csv diff for
        # mimetype text/csv, text diff for mimetype text/plain, etc.
        # Realistically, only one diff setting will apply, return a value as soon as
        # we find a match
        for diff_settings in diff_enabled:
            diff_event, ignore = self.process_diff_event(
                index, doc_id, r, diff_settings)
            if not ignore:
                # Default insertion: no need for an update in the case of events
                self.logger.debug(
                    f"Append event for stream {index+DIFF_SUFFIX}: {doc_id}")
                return {
                    **{"_op_type": "index", "_index": index+DIFF_SUFFIX, "_source": diff_event, "_id": diff_event["id"]},
                    **build_fields(self.stream_name+DIFF_SUFFIX, metadata_fields, diff_event, self.logger),
                }
            # else:
            #     self.logger.info(f"Ignore diff (no change): stream {index+DIFF_SUFFIX}: {diff_event} ")
        return None

    def process_diff_event(self, main_index: str, doc_id: str, new_doc: Dict[str, str | Dict[str, str] | int], diff_config: Dict) -> Tuple[Dict, bool]:
        # Raise an exception if the field does not exist in the doc
        if diff_config.get(EVENT_TIME_KEY, '').lower() == "autogenerated":
            evt_time = datetime.datetime.now().isoformat()
        else:
            evt_time = new_doc.get(diff_config.get(EVENT_TIME_KEY))
        ignored_fields = []
        ignored_fields.extend(diff_config.get(IGNORED_FIELDS, []))
        ignored_fields.extend(DEFAULT_IGNORED_FIELDS)
        specific_diff_process = diff_config.get(SPECIFIC_DIFF_PROCESS, "")
        specific_diff_process_data_field  = diff_config.get(SPECIFIC_DIFF_PROCESS_DATA_FIELD, "")
        specific_diff_process_filter_field  = diff_config.get(SPECIFIC_DIFF_PROCESS_FILTER_FIELD, "")
        specific_diff_process_filter_value  = diff_config.get(SPECIFIC_DIFF_PROCESS_FILTER_VALUE, "")

        diff_event = {}
        diff_event["id"] = f"{doc_id}-event-{evt_time}"
        diff_event["main_doc_key"] = doc_id
        diff_event["event_ts"] = evt_time
        # Inherit the sdc sequence from the new doc
        diff_event["_sdc_sequence"] = new_doc.get("_sdc_sequence")
        diff_event["_sdc_extracted_at"] = new_doc.get("_sdc_extracted_at")
        diff_event["_sdc_batched_at"] = new_doc.get("_sdc_batched_at")

        original_doc = self.fetch_cached_doc(index=main_index, doc_id=doc_id)

        ignore = False
        if not specific_diff_process:
            diff_result = dict_diff(original_doc, new_doc, ignored_fields)
            diff_event["diff_type"] = diff_result["diff_type"]
            diff_event["from"] = diff_result["from"]
            diff_event["to"] = diff_result["to"]
            if len(diff_event.get("from", {}).keys()) == 0 and len(diff_event.get("to", {}).keys()) == 0:
                ignore = True
        else:
            diff_result, ignore = self.specific_diff(original_doc,
                                                    new_doc,
                                                    specific_diff_process,
                                                    specific_diff_process_data_field,
                                                    specific_diff_process_filter_field,
                                                    specific_diff_process_filter_value)
            diff_event["changes"] = diff_result
        
        # Also include the full docs, to make future queries easier (it's slow if we need to access the original doc for context each time)
        # However, only include the resulting doc because otherwise the diff doc would be too heavy!
        # diff_event["original_doc"] = original_doc
        diff_event["new_doc"] = new_doc
        diff_event = {k: v for k, v in diff_event.items() if v !=
                      None and v != ""}

        return diff_event, ignore

    # Only 16 items in the cache
    @lru_cache(maxsize=16)
    def fetch_cached_doc(self, index: str, doc_id: str):
        try:
            res = self.client.get(index=index, id=doc_id)
            original_doc = res["_source"]
        except NotFoundError:
            original_doc = {}
        except Exception as e:
            # Should not happen -> raise
            self.logger.error(
                f"Error while fetching document {doc_id} from {index} in order to build diff: {e}")
            raise e
        return original_doc

    def specific_diff(self, old, new, specific_diff_process, specific_diff_process_data_field, specific_diff_process_filter_field, specific_diff_process_filter_value) -> Tuple[Dict[str, Any], bool]:
        diff = {}
        old_data = pydash.get(old, specific_diff_process_data_field, "")
        new_data = pydash.get(new, specific_diff_process_data_field, "")
        if pydash.get(new, specific_diff_process_filter_field, "") != specific_diff_process_filter_value:
            return diff, True

        ignore = False
        if specific_diff_process == SPECIFIC_DIFF_PROCESS_CSV:
            diff, ignore = spreadsheet_diff(old_data,new_data)
        elif specific_diff_process == SPECIFIC_DIFF_PROCESS_TEXT:
            diff, ignore = text_diff(old_data,new_data)
        elif specific_diff_process == SPECIFIC_DIFF_PROCESS_TEXT_LINE:
            diff, ignore = text_diff_line(old_data,new_data)
        elif specific_diff_process == SPECIFIC_DIFF_PROCESS_FLAT:
            diff, ignore = flat_diff(old_data,new_data)
        else:
            self.logger.error(f"Unsupported specific diff: {specific_diff_process}")
        return diff, ignore

def dict_diff(old, new, ignored_fields):
    diff = {"from": {}, "to": {}, "diff_type": SPECIFIC_DIFF_PROCESS_DICT}

    all_keys = set(old.keys()) | set(new.keys())

    for key in all_keys:
        if any(re.match(pattern, key) for pattern in ignored_fields):
            continue
        if key in old and key in new:
            # If the value is a dictionary, compare recursively
            if isinstance(old[key], dict) and isinstance(new[key], dict):
                nested_diff = dict_diff(old[key], new[key], ignored_fields)
                if nested_diff["from"] or nested_diff["to"]:  # If there's a change
                    diff["from"][key] = nested_diff["from"]
                    diff["to"][key] = nested_diff["to"]
            elif old[key] != new[key]:  # If the value has changed
                diff["from"][key] = old[key]
                diff["to"][key] = new[key]
        elif key in new:  # If the key is an addition
            diff["to"][key] = new[key]
        elif key in old:  # If the key is a removal
            diff["from"][key] = old[key]

    return diff

def spreadsheet_diff(csv_string1, csv_string2):
    def safe_read_csv(csv_string):
        if not csv_string.strip():
            return pd.DataFrame()
        
        max_columns = 0
        for line in csv.reader(io.StringIO(csv_string)):
            max_columns = max(max_columns, len(line))

        return pd.read_csv(
            io.StringIO(csv_string),
            dtype=str,
            keep_default_na=False,
            quotechar='"',
            escapechar='\\',
            names=range(max_columns),
            header=None,
            on_bad_lines='warn'
        )

    df1 = safe_read_csv(csv_string1)
    df2 = safe_read_csv(csv_string2)

    # Ensure both DataFrames have the same number of columns
    max_cols = max(df1.shape[1], df2.shape[1]) if not df1.empty or not df2.empty else 0
    df1 = df1.reindex(columns=range(max_cols), fill_value='')
    df2 = df2.reindex(columns=range(max_cols), fill_value='')
    
    changes = []
    
    def is_significant_change(val1, val2):
        # Consider change significant if the values are different
        # and neither is an empty string or NaN-like value
        return (val1 != val2) and not (
            (pd.isna(val1) or val1 == '') and (pd.isna(val2) or val2 == '')
        )

    # Compare each cell
    for i in range(max(df1.shape[0], df2.shape[0])):
        for j in range(max_cols):
            val1 = df1.iloc[i, j] if i < df1.shape[0] and j < df1.shape[1] else ''
            val2 = df2.iloc[i, j] if i < df2.shape[0] and j < df2.shape[1] else ''
            
            if is_significant_change(val1, val2):
                changes.append({
                    'row': i + 1,  # +1 for human-readable row numbers
                    'column': j + 1,  # +1 for human-readable column numbers
                    'old_value': val1,
                    'new_value': val2,
                })
    
    # Analyze structural changes
    rows_added = df2.shape[0] - df1.shape[0]
    cols_added = df2.shape[1] - df1.shape[1]
    
    # Detect moved content
    # def find_moved_content(source_df, target_df):
    #     moved = []
    #     for i, row in source_df.iterrows():
    #         row_str = ' '.join(row[row.astype(bool)].astype(str))
    #         for j, target_row in target_df.iterrows():
    #             if i != j:  # Avoid comparing the same row index
    #                 target_row_str = ' '.join(target_row[target_row.astype(bool)].astype(str))
    #                 similarity = SequenceMatcher(None, row_str, target_row_str).ratio()
    #                 if similarity == 1:
    #                     moved.append((i, j))
    #                     break
    #     return moved
    # Moved content processing doesn't work very well for now - TODO: improve this
    # moved_content = find_moved_content(df1, df2) if not df1.empty and not df2.empty else []
    
    ignore_change = len(changes) == 0 and rows_added == 0 and cols_added == 0

    return {
        "diff_type": SPECIFIC_DIFF_PROCESS_CSV,
        "cell_changes": changes,
        "structural_changes": {
            "rows_added": rows_added,
            "columns_added": cols_added,
        },
        # 'moved_content': moved_content
    }, ignore_change



def text_diff(text1, text2):
    # Instead of doing a basic diff, doing a diff after word split allows us to better identify
    # changes. For example, "Hello?" -> "Goodbye" might be interpreted as a series of remove+add 
    # instead of one single replace.
    def split_into_words(text):
        return re.findall(r"\S+|\s+", text)

    lines1 = text1.splitlines()
    lines2 = text2.splitlines()
    matcher = difflib.SequenceMatcher(None, lines1, lines2)

    result = []
    line_number1 = 1
    line_number2 = 1

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            # Ignore lines which have not changed
            line_number1 += 1
            line_number2 += 1
            # for line in lines1[i1:i2]:
            #     result.append({"type": "unchanged", "text": line})
        elif tag in ["replace", "delete", "insert"]:
            for line1, line2 in zip_longest(lines1[i1:i2], lines2[j1:j2], fillvalue=""):
                if line1 == line2:
                    # Ignore unchanged sections within a given line
                    line_number1 += 1
                    line_number2 += 1
                    # result.append({"type": "unchanged", "text": line1})
                else:
                    words1 = split_into_words(line1)
                    words2 = split_into_words(line2)
                    word_matcher = difflib.SequenceMatcher(None, words1, words2)
                    line_diff = []
                    for word_tag, wi1, wi2, wj1, wj2 in word_matcher.get_opcodes():
                        if word_tag == "equal":
                            line_diff.append({"type": "unchanged", "text": "".join(words1[wi1:wi2])})
                        elif word_tag == "delete":
                            line_diff.append({"type": "removed", "text": "".join(words1[wi1:wi2])})
                        elif word_tag == "insert":
                            line_diff.append({"type": "added", "text": "".join(words2[wj1:wj2])})
                        elif word_tag == "replace":
                            line_diff.append({"type": "removed", "text": "".join(words1[wi1:wi2])})
                            line_diff.append({"type": "added", "text": "".join(words2[wj1:wj2])})
                    result.append({"line_number": line_number2,  "diff": line_diff, "result": line2})
                    if line1:
                        line_number1 += 1
                    if line2:
                        line_number2 += 1

    ignore_change = len(result) == 0
    ret = {"changes": result, "diff_type": SPECIFIC_DIFF_PROCESS_TEXT}
    return ret, ignore_change

def text_diff_line(text1, text2):
    lines1 = text1.splitlines()
    lines2 = text2.splitlines()
    matcher = difflib.SequenceMatcher(None, lines1, lines2)

    result = []
    line_number1 = 1
    line_number2 = 1

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            line_number1 += i2 - i1
            line_number2 += j2 - j1
        elif tag == "replace":
            result.append(
                {
                    "type": "changed",
                    "line_number": line_number1,
                    "text_from": lines1[i1:i2],
                    "text_to": lines2[j1:j2],
                }
            )
            line_number1 += i2 - i1
            line_number2 += j2 - j1
        elif tag == "delete":
            for line in lines1[i1:i2]:
                result.append({"type": "removed", "line_number": line_number1, "text": line})
                line_number1 += 1
        elif tag == "insert":
            for line in lines2[j1:j2]:
                result.append({"type": "added", "line_number": line_number2, "text": line})
                line_number2 += 1

    ignore_change = len(result) == 0
    ret = {"changes": result, "diff_type": SPECIFIC_DIFF_PROCESS_TEXT_LINE}
    return ret, ignore_change

def flat_diff(text1, text2):
    # Just in case, try a line diff to see if this change should be ignored
    _, ignore = text_diff_line(text1, text2)

    # Don't do any diff, but keep in an easily accessible format both documents
    return {
        "from": text1,
        # No need to include the "to" here: it's already available in the new_doc field
        # "to": text2,
        "diff_type": SPECIFIC_DIFF_PROCESS_FLAT
    }, ignore