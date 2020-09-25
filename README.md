# amundsengremlin
[![PyPI version](https://badge.fury.io/py/amundsen-gremlin.svg)](https://badge.fury.io/py/amundsen-gremlin)
[![License](https://img.shields.io/:license-Apache%202-blue.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](#developer-guide)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://amundsenworkspace.slack.com/join/shared_invite/enQtNTk2ODQ1NDU1NDI0LTc3MzQyZmM0ZGFjNzg5MzY1MzJlZTg4YjQ4YTU0ZmMxYWU2MmVlMzhhY2MzMTc1MDg0MzRjNTA4MzRkMGE0Nzk)

Amundsen Gremlin contains code to use AWS Neptune as the graph backend for Amundsen. Specifically it uploads two CSVs -- one for vertices, one for edges -- to an S3 bucket, then tells the [bulk loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html) to import those into the graph database. In order to prevent duplicate vertexes/edges, we specify the key of each.

## Requirements

It can be used with Python 3.6 except for `async_consume_in_chunks` which relies on Python 3.7 asyncio functionality.

Prerequisites include a configured Neptune instance and an S3 bucket.

## Example Code
This can be used by databuilder jobs to load data into the graph. Example code for batching:

```python
    def load_tables(self, *, table_data: Iterable[Table], batch_size: int = 200000,
                    batch_metric: LoadTablesBatchMetric = LoadTablesBatchMetric.NUMBER_OF_NODES) -> int:
        """
        lazily loads Tables in chunks of batch_size
        :param table_data: the Iterable (possibly a Generator or stream) of Tables
        :param batch_size: the maximum chunk size to process, or <= 0 if process all at once
        :param batch_metric: what metric to count for chunks?  number of tables or number of nodes?
        """
        return consume_in_chunks(stream=table_data, n=batch_size, metric=batch_metric.value,
                                 consumer=self._load_some_tables)

    async def async_load_tables(self, *, table_data: AsyncIterator[Table], batch_size: int = 5000) -> int:
        """
        lazily loads Tables in chunks of batch_size
        """
        return await async_consume_in_chunks(stream=table_data, n=batch_size, consumer=self._load_some_tables)

    def _load_some_tables(self, data: Iterable[Table]) -> None:
        _data = list(data)
        entities = GetGraph.table_entities(table_data=_data, g=self.neptune_graph_traversal_source_factory())
        self.neptune_bulk_loader_api.bulk_load_entities(entities=entities)
```

## AWS Configuration Guide
Coming Soon...

## Instructions to configure venv
Virtual environments for python are convenient for avoiding dependency conflicts.
The `venv` module built into python3 is recommended for ease of use, but any managed virtual environment will do.
If you'd like to set up venv in this repo:
```bash
$ venv_path=[path_for_virtual_environment]
$ python3 -m venv $venv_path
$ source $venv_path/bin/activate
$ pip install -r requirements.txt
```

If something goes wrong, you can always:
```bash
$ rm -rf $venv_path
```

## Roundtrip tests
The roundtrip tests hit the Neptune backend directly, which requires a valid Neptune configuration. As amundsen-gremlin CI does not currently have AWS configured, these tests do not run by default.

In order to run the roundtrip tests:
```bash
$ python -m pytest --roundtrip .
```
