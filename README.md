[![Tests](https://github.com/DataShades/ckanext-federated-index/workflows/Tests/badge.svg?branch=main)](https://github.com/DataShades/ckanext-federated-index/actions)

# ckanext-federated-index

Lightweight solution for storing and searching remote datasets locally and
redirecting to the original portal upon when dataset details page is opened.

Current extension is similar to
[ckanext-harvest](https://github.com/ckan/ckanext-harvest). Main differences
are:

* ckanext-harvest is a generic solution for harvesting data from any kind of
  source. ckanext-federated-index works only with CKAN instances
* ckanext-harvest uses background processes for harvesting. It's more
  sophisticated, customizable and flexible, but at the same time, it's more
  complex. ckanext-federated-index relies only on CKAN API and can be triggered
  via HTTP requests, CLI commands or cron-tasks without additional complexity.
* ckanext-harvest creates copies of remote datasets locally. This is more
  appropriate if you want to create references to these dataasets, edit them or
  modify local copies. ckanext-federated-index adds datasets to search index,
  but does not create real datasets locally. So you can search these datasets,
  but cannot open them locally. Instead you can use original URL of the dataset
  and redirect user to the original portal.

As result, ckanext-federated-index works best if you are building lightweight
aggregator of data from multiple portals, but do not provide any view or edit
capabilities. ckanext-harvest suits better for any other case, as it basically
allows you to do anything with remote datasets.


## Requirements

Compatibility with core CKAN versions:

| CKAN version | Compatible? |
|--------------|-------------|
| 2.9          | no          |
| 2.10         | yes         |
| 2.11         | yes         |


## Installation

To install ckanext-federated-index:

1. Install it via pip:
   ```sh
   pip install ckanext-federated-index
   ```

1. Add `federated-index` to the `ckan.plugins` setting in your CKAN
   config file.

## Usage

To index remote datasets, you need to configure one or multiple federation
**profiles**. Each profile describes the remote portal and defines, how its
data is fetched and stored.

Each profile must have a unique name and its configuration options are defined
as `ckanext.federated_index.profile.<PROFILE_NAME>.<OPTION>`. For example, if
you decided to index [demo.ckan.org](https://demo.ckan.org) and want to use
name `demo` for it, you have to add the following option to the config file:
```ini
ckanext.federated_index.profile.demo.url = https://demo.ckan.org
```

If, in addition to URL you want to specify an API Token for requests:
```ini
ckanext.federated_index.profile.demo.url = https://demo.ckan.org
ckanext.federated_index.profile.demo.api_key = 123-abc
```
All available config options are mentioned in [config settings](#config-settings) section.

When profile is configured, the only thin you need to do is to refresh portal
data. If you local and remote portals have similar metadata schemas, it should
work without any additional efforts. If schemas are different, check [advanced
usage](#advanced-usage) section.

```sh
ckanapi action federated_index_profile_refresh profile=demo index=true
```

## Advanced usage

### Align metadata schemas

Usually, when remote portal is heavily customized and defines a lot of custom
metadata fields, the easiest option is to drop all fields that are not defined
in the local metadata schema. It can be done via config option:

```ini
ckanext.federated_index.align_with_local_schema = true
```

It it's not enough, you can hook into indexation process and alter dataset
dictionary before it's sent to search index. For this purpose you can use
`IFederatedIndex` interface:

```python

import ckan.plugins as p
from ckanext.federated_index.interfaces import IFederatedIndex
from ckanext.federated_index.shared import Profile

class CustomFederatedIndexPlugin(p.SingletonPlugin):
    p.implements(interfaces.IFederatedIndex, inherit=True)

    def federated_index_before_index(
        self,
        pkg_dict: dict[str, Any],
        profile: Profile,
    ) -> dict[str, Any]:

        # modify data. For example, remove all tags with vocabulary_id,
        # because local instance usually does not have same vocabulary IDs
        pkg_dict["tags"] = [
            t for t in pkg_dict.setdefault("tags", []) if "vocabulary_id" not in t
        ]

        return pkg_dict

```

### Fetch datasets that are newer than the newest locally-indexed dataset

On initial synchronization you often need to pull all the datasets from remote
portal. But after that you are only interested in datasets with
`metadata_modified` value greater that the newest `metadata_modified` among
synchronized datasets. Basically, you want to fetch only updated datasets to
speed-up the process. To achieve it, add `since_last_refresh` flag to the
action that refreshes the index:

```sh
ckanapi action federated_index_profile_refresh profile=demo index=true since_last_refresh=true
```

### Configure remote data fetch process

ckanext-federated-index fetches data from the remote portal using
`package_search` API action with default parameters. If you want to increase
the number of packages fetched via single request, or filter out certains
datasets using `q`/`fq`, you can add search configuration to profile via
settings:

```ini
ckanext.federated_index.profile.demo.extras = {"search_payload": {"rows": 100, "q": "test"}}
```

`extras` option of the profile contains a valid JSON object with additional
settings of the profile. `search_payload` specifies default parameters used for
`package_search`.

In addition to it, if you want to use custom search payload only once, you can
pass `search_payload` to the refresh action:

```sh
ckanapi action federated_index_profile_refresh profile=demo index=true search_payload='{"q": "test"}'
```

### Configure storage for remote data

By default, remote data stored inside a separate DB table. It allows you to
pull remote data once, re-build index of remote packages multiple times without
making additional requests to the remote portal. DB table is chosen as default
value because it can efficiently use space, allows fast access to the data and
is available on every CKAN instance, as CKAN doesn't work without DB.

But there are other storage types and every federation profile can be
configured to use a different storage type via `extras` option:

```ini
ckanext.federated_index.profile.demo.extras = {"storage": {"type": "redis"}}
```

`type` key is a required member of the `storage` object. Depending on the
storage type, other keys can be supported as well. For example, filesystem
storage allows you to specify path, where the data is stored:

```ini
ckanext.federated_index.profile.demo.extras = {"storage": {"type": "fs", "path": "/tmp/demo_profile"}}
```

Storage types:

* `db`: default storage. Keeps data inside a custom table created via migration
* `fs`: keeps data as separate JSON files in the filesystem. By default files
  created under `ckan.storage_path`/federated_index/PROFILENAME. Path can be
  changed via `path` option.
* `redis`: keeps data inside Redis
* `sqlite`: keeps data as separate SQLite DB for eachprofile. By default
   database is created at
   `ckan.storage_path`/federated_index/PROFILENAME.sqlite3.db. Path can be
   changed via `url` option.


## Config settings

```ini
# Remove from dataset any field that is not defined in the local dataset
# schema.
# (optional, default: false)
ckanext.federated_index.align_with_local_schema = false

# Redirect user to the original dataset URL when user opens federated dataset
# that is not recorded in local DB.
# (optional, default: true)
ckanext.federated_index.redirect_missing_federated_datasets = true

# Endpoints that are affected by `redirect_missing_federated_datasets` config
# option.
# (optional, default: dataset.read)
ckanext.federated_index.dataset_read_endpoints = dataset.read dataset.edit

# Name of the dataset extra field that holds original URL of the federated
# dataset.
# (optional, default: federated_index_remote_url)
ckanext.federated_index.index_url_field = federated_index_remote_url

# Name of the dataset extra field that holds federation profile name.
# (optional, default: federated_index_profile)
ckanext.federated_index.index_profile_field = federated_index_profile

# URL of the federation profile.
ckanext.federated_index.profile.<profile>.url = https://demo.ckan.org

# API Token for the federation profile.
ckanext.federated_index.profile.<profile>.api_key = 123-abc

# Extra configuration for federation profile. Must be a valid JSON object
# with the following keys:
#  * search_payload: payload sent to remote portal with
#    `package_search` API action when profile is refreshed
#  * storage: storage configuration for remote data. Requires `type`
#    parameter with one of the following values: redis, db, sqlite, fs.
ckanext.federated_index.profile.<profile>.extras = {"search_payload": {"rows": 100}, "storage": {"type": "fs"}}

# Request timeout for remote portal requests.
ckanext.federated_index.profile.<profile>.timeout = 5

```

## Developer installation

To install ckanext-federated-index for development, activate your CKAN virtualenv and
do:

```sh
git clone https://github.com/DataShades/ckanext-federated-index.git
cd ckanext-federated-index
pip install -e.
```

## Tests

To run the tests, do:

```sh
pytest
```

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
