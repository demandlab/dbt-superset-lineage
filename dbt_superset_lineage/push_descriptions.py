"""
push_descriptions.py

Orchestrates reading the dbt manifest and pushing table and column descriptions
to Superset via its REST API. Converts Markdown descriptions in your dbt YAML
into plain text and updates both table‐level and column‐level metadata in Superset.
"""

import json
import logging
import re
import time
from bs4 import BeautifulSoup
from markdown import markdown
from requests import HTTPError

from .superset_api import Superset

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def get_datasets_from_superset(superset, superset_db_id):
    """
    Retrieve all physical datasets from Superset, optionally filtered by database ID.

    Iterates through paginated Superset API responses and collects datasets
    whose 'kind' is 'physical' and whose database matches `superset_db_id` (if provided).

    Args:
        superset (Superset): An initialized Superset API client.
        superset_db_id (int or None): If set, only datasets in this database ID
            will be returned. If None, all physical datasets are returned.

    Returns:
        List[Dict]: A list of dicts with keys:
            - 'id' (int): Superset dataset ID.
            - 'key' (str): Unique key in the form "schema.table_name".

    Raises:
        AssertionError: If no datasets are found, or if a duplicate
            "schema.table_name" key is detected across pages/databases.
    """
    logging.info("Getting physical datasets from Superset.")

    page_number = 0
    datasets = []
    datasets_keys = set()

    while True:
        logging.info("Getting page %d.", page_number + 1)
        payload = {"q": json.dumps({"page": page_number, "page_size": 100})}
        res = superset.request("GET", "/dataset/", params=payload)
        result = res["result"]

        if not result:
            break

        for r in result:
            kind = r["kind"]
            database_id = r["database"]["id"]

            if (
                kind == "physical"
                and (superset_db_id is None or database_id == superset_db_id)
            ):
                dataset_id = r["id"]
                name = r["table_name"]
                schema = r["schema"]
                dataset_key = f"{schema}.{name}"

                # fail if it breaks uniqueness constraint
                assert dataset_key not in datasets_keys, (
                    f"Dataset {dataset_key} is a duplicate name "
                    "(schema + table) across databases. "
                    "This would result in incorrect matching between "
                    "Superset and dbt. To fix this, remove duplicates "
                    "or add the `superset_db_id` argument."
                )

                datasets_keys.add(dataset_key)
                datasets.append({"id": dataset_id, "key": dataset_key})

        page_number += 1

    assert datasets, "There are no datasets in Superset!"
    return datasets


def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    """
    Parse a dbt manifest and collect table and column metadata.

    Reads both 'nodes' and 'sources' entries from the manifest JSON, filters by
    `dbt_db_name` if provided, and returns a mapping from "schema.name" to
    its column definitions and table description.

    Args:
        dbt_manifest (dict): The loaded JSON from dbt's target/manifest.json.
        dbt_db_name (str or None): If set, only tables in this `database` field
            will be included. If None, all tables are included.

    Returns:
        Dict[str, Dict]: Mapping of table_key -> metadata, where:
            table_key (str): "schema.name"
            metadata (dict):
                - 'columns' (dict): column_name -> column metadata dict.
                - 'description' (str): table-level description.

    Raises:
        AssertionError: If the manifest is empty or if duplicate keys
            appear across 'nodes' and 'sources'.
    """
    tables = {}
    for table_type in ["nodes", "sources"]:
        # Use '_' for the unused long key to satisfy linting
        for _, table in dbt_manifest[table_type].items():
            name = table["name"]
            schema = table["schema"]
            database = table["database"]
            table_key = f"{schema}.{name}"
            columns = table["columns"]
            description = table["description"]

            if dbt_db_name is None or database == dbt_db_name:
                assert table_key not in tables, (
                    f"Table {table_key} is a duplicate name "
                    "(schema + table) across databases. "
                    "This would result in incorrect matching between "
                    "Superset and dbt. To fix this, remove duplicates "
                    "or add the `dbt_db_name` argument."
                )
                tables[table_key] = {"columns": columns, "description": description}

    assert tables, "Manifest is empty!"
    return tables


def refresh_columns_in_superset(superset, dataset_id):
    """
    Instruct Superset to refresh the column list for a given dataset.

    Args:
        superset (Superset): Superset API client.
        dataset_id (int): The dataset ID in Superset.
    """
    logging.info("Refreshing columns in Superset.")
    superset.request("PUT", f"/dataset/{dataset_id}/refresh")


def add_superset_columns(superset, dataset):
    """
    Fetch the latest column and description metadata for a dataset.

    Calls Superset's GET /dataset/{id} endpoint and populates:
      - dataset['columns']
      - dataset['description']
      - dataset['owners']

    Args:
        superset (Superset): Superset API client.
        dataset (dict): A dict containing at least 'id' key.

    Returns:
        dict: The same dataset dict, now with 'columns', 'description',
              and 'owners' populated from Superset.
    """
    logging.info("Pulling fresh columns info from Superset.")
    res = superset.request("GET", f"/dataset/{dataset['id']}")
    result = res["result"]

    dataset["columns"] = result["columns"]
    dataset["description"] = result["description"]
    dataset["owners"] = result["owners"]
    return dataset


def convert_markdown_to_plain_text(md_string):
    """
    Convert a Markdown string into a single-line plain-text string.

    1. Renders Markdown to HTML.
    2. Strips <pre> and <code> snippets.
    3. Extracts text via BeautifulSoup.
    4. Collapses whitespace to a single space.
    5. Normalizes arrows and <null> tags.

    Args:
        md_string (str): The Markdown-formatted string.

    Returns:
        str: A plain-text, single-line version of the input.
    """
    html = markdown(md_string)
    html = re.sub(r"<pre>(.*?)</pre>", " ", html)
    html = re.sub(r"<code>(.*?)</code >", " ", html)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    single_line = re.sub(r"\s+", " ", text)
    single_line = re.sub("→", "->", single_line)
    single_line = re.sub("<null>", '"null"', single_line)
    return single_line


def merge_columns_info(dataset, tables):
    """
    Merge Superset column metadata with dbt column overrides.

    For each column in Superset:
      - If a dbt description exists (and the column has no expression),
        convert and override the Superset description.
      - If a dbt meta.label exists, convert and override Superset verbose_name.

    Builds a new list in dataset['columns_new'] and preserves
    the original Superset owners under 'owners_new'.

    Args:
        dataset (dict): A Superset dataset dict with keys:
            - 'key' (str): "schema.name"
            - 'columns' (list): list of column dicts from Superset
            - 'description' (str), 'owners' (list)
        tables (dict): The output of get_tables_from_dbt(), keyed by 'key'.

    Returns:
        dict: The input dataset dict, extended with:
            - 'columns_new' (list): updated column metadata
            - 'description_new' (str): final table-level description
            - 'owners_new' (list): list of owner IDs
    """
    logging.info("Merging columns info for dataset %s.", dataset["key"])
    key = dataset["key"]
    sst_columns = dataset["columns"]
    dbt_columns = tables.get(key, {}).get("columns", {})
    sst_description = dataset.get("description")
    dbt_description = tables.get(key, {}).get("description")
    sst_owners = dataset.get("owners", [])

    columns_new = []
    for sst_col in sst_columns:
        col_name = sst_col["column_name"]
        col_id = sst_col["id"]
        expr = sst_col.get("expression") or None

        desc = sst_col.get("description")
        label = sst_col.get("verbose_name")

        if col_name in dbt_columns and expr is None:
            dbt_col = dbt_columns[col_name]
            if "description" in dbt_col:
                desc = convert_markdown_to_plain_text(dbt_col["description"])
            meta = dbt_col.get("meta", {})
            if "label" in meta:
                label = convert_markdown_to_plain_text(meta["label"])

        columns_new.append(
            {
                "column_name": col_name,
                "id": col_id,
                "description": desc,
                "verbose_name": label,
            }
        )

    dataset["columns_new"] = columns_new
    dataset["description_new"] = (
        convert_markdown_to_plain_text(dbt_description)
        if dbt_description is not None
        else sst_description
    )
    dataset["owners_new"] = [o["id"] for o in sst_owners]
    return dataset


def check_columns_equal(lst1, lst2):
    """
    Compare two lists of column dicts by their 'id' fields.

    Args:
        lst1 (list): First list of column dicts.
        lst2 (list): Second list of column dicts.

    Returns:
        bool: True if both lists have the same set of IDs (order-insensitive).
    """
    return sorted(lst1, key=lambda c: c["id"]) == sorted(lst2, key=lambda c: c["id"])


def pause_after_update(superset_pause_after_update):
    """
    Pause execution for a given number of seconds, if configured.

    Used to allow Superset time to process column-refresh or updates.

    Args:
        superset_pause_after_update (int): Number of seconds to sleep.
    """
    if superset_pause_after_update:
        logging.info(
            "Pausing the script to allow Superset to catch up (%d seconds).",
            superset_pause_after_update,
        )
        time.sleep(superset_pause_after_update)
        logging.info("Resuming the script.")


def put_descriptions_to_superset(superset, dataset, superset_pause_after_update):
    """
    Push updated table and column descriptions to Superset.

    Compares the new descriptions/labels with the old ones, and only issues
    a PUT request if something has actually changed.

    Args:
        superset (Superset): Superset API client.
        dataset (dict): A dataset dict with 'columns_new', 'description_new',
                        and 'owners_new' keys, plus original 'columns' &
                        'description'.
        superset_pause_after_update (int): Seconds to pause after the update.

    Raises:
        HTTPError: If the Superset API returns an error.
    """
    logging.info("Putting model and column descriptions into Superset.")
    description_new = dataset["description_new"]
    columns_new = dataset["columns_new"]
    owners_new = dataset["owners_new"]

    description_old = dataset["description"]
    columns_old = [
        {"column_name": col["column_name"], "id": col["id"], "description": col["description"]}
        for col in dataset["columns"]
    ]

    if description_new != description_old or not check_columns_equal(columns_new, columns_old):
        payload = {"description": description_new, "columns": columns_new, "owners": owners_new}
        superset.request(
            "PUT",
            f"/dataset/{dataset['id']}?override_columns=false",
            json=payload,
        )
        pause_after_update(superset_pause_after_update)
    else:
        logging.info("Skipping PUT; nothing to update.")


def main(
    dbt_project_dir,
    dbt_db_name,
    superset_url,
    superset_db_id,
    superset_refresh_columns,
    *,
    superset_pause_after_update,
    superset_access_token,
    superset_refresh_token,
):
    """
    Entry point: read dbt manifest and push descriptions/labels to Superset.

    Workflow:
    1. Initialize Superset API client.
    2. Fetch all physical Superset datasets (filtered by superset_db_id).
    3. Load dbt manifest from target/manifest.json.
    4. Filter to only those datasets also present in dbt.
    5. For each matching dataset:
       a. (Optional) refresh Superset columns.
       b. Fetch current Superset columns & metadata.
       c. Merge dbt overrides into new metadata.
       d. Push updated descriptions if changed.

    Args:
        dbt_project_dir (str): Path to your dbt project root.
        dbt_db_name (str or None): Only include dbt tables in this database name.
        superset_url (str): Base URL for Superset instance (no trailing /api/v1).
        superset_db_id (int or None): Only include Superset datasets in this database ID.
        superset_refresh_columns (bool): Whether to call the refresh endpoint first.
        superset_pause_after_update (int): Seconds to sleep after each update.
        superset_access_token (str): Superset access token for authentication.
        superset_refresh_token (str or None): Superset refresh token, if using.
    """
    assert (
        superset_access_token is not None or superset_refresh_token is not None
    ), "Add `SUPERSET_ACCESS_TOKEN` or `SUPERSET_REFRESH_TOKEN` to env or CLI."

    superset = Superset(
        f"{superset_url}/api/v1",
        access_token=superset_access_token,
        refresh_token=superset_refresh_token,
    )

    logging.info("Starting the script!")
    sst_datasets = get_datasets_from_superset(superset, superset_db_id)
    logging.info(
        "There are %d physical datasets in Superset overall.", len(sst_datasets)
    )

    manifest_path = f"{dbt_project_dir}/target/manifest.json"
    with open(manifest_path, encoding="utf-8") as f:
        dbt_manifest = json.load(f)

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)
    sst_filtered = [d for d in sst_datasets if d["key"] in dbt_tables]
    logging.info("There are %d Superset datasets matching dbt.", len(sst_filtered))

    for idx, sst_dataset in enumerate(sst_filtered, start=1):
        logging.info("Processing dataset %d/%d.", idx, len(sst_filtered))
        try:
            if superset_refresh_columns:
                refresh_columns_in_superset(superset, sst_dataset["id"])
                pause_after_update(superset_pause_after_update)

            sst_cols = add_superset_columns(superset, sst_dataset)
            merged = merge_columns_info(sst_cols, dbt_tables)
            put_descriptions_to_superset(
                superset, merged, superset_pause_after_update
            )
        except HTTPError as e:
            logging.error(
                "Dataset ID=%d update failed.", sst_dataset["id"], exc_info=e
            )

    logging.info("All done!")
