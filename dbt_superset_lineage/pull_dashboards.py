"""
pull_dashboards.py

Extract dashboard definitions and their underlying datasets from Superset,
and convert them into dbt exposures. Parses SQL for lineage via SQLFluff
with regex fallback. Generates a dbt-v1.3-compatible exposures YAML file.
"""

import json
import logging
import re
from pathlib import Path
from requests import HTTPError

import ruamel.yaml
import sqlfluff

from .superset_api import Superset

# Configure root logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logging.getLogger("sqlfluff").setLevel(logging.WARNING)


def crawl_recursive(seq, key):
    """
    Recursively yield values for `key` in a nested dict/list.

    Args:
        seq (dict | list): The structure to traverse.
        key (str): The dict key to match.

    Yields:
        The values found under `key`.
    """
    if isinstance(seq, dict):
        for k, v in seq.items():
            if k == key:
                yield v
            else:
                yield from crawl_recursive(v, key)
    elif isinstance(seq, list):
        for item in seq:
            yield from crawl_recursive(item, key)


def get_tables_from_sql_fluff(sql, dialect):
    """
    Use SQLFluff to extract table references from SQL.

    Args:
        sql (str): The SQL string to parse.
        dialect (str): The SQLFluff dialect to use.

    Returns:
        set[str]: Table references like 'schema.table' or 'table'.
    """
    parsed = sqlfluff.parse(sql=sql, dialect=dialect)
    refs = crawl_recursive(parsed, "table_reference")

    tables = set()
    for ident in ("naked_identifier", "quoted_identifier"):
        fragments = [list(crawl_recursive(r, ident)) for r in refs]
        cleaned = [
            ".".join(frag).replace('"', "").lower()
            for frag in fragments
            if len(frag) >= 2
        ]
        tables.update(cleaned)
    return tables


def get_tables_from_sql_simple(sql):
    """
    Fallback regex extraction of table names for simple SQL.

    Args:
        sql (str): The SQL string to analyze.

    Returns:
        set[str]: Table names found via regex.
    """
    # strip comments
    sql = re.sub(r"(--.*)|(#.*)", "", sql)
    sql = re.sub(r"(/\*(.|\n)*\*/)", "", sql)
    sql = re.sub(r"\s+", " ", sql).lower()

    pattern = re.compile(
        r"\b(from|join)\b\s+(\"?(\w+)\"?(\.))?\"?(\w+)\"?\b"
    )
    matches = pattern.findall(sql)
    tables = {
        f"{m[2]}.{m[4]}" if m[2] else m[4]
        for m in matches
        if m[4] != "unnest"
    }
    return tables


def get_tables_from_sql(sql, dialect):
    """
    Try SQLFluff parse, else regex fallback, to list tables.

    Args:
        sql (str): SQL to scan.
        dialect (str): SQLFluff dialect.

    Returns:
        list[str]: Identified table references.
    """
    try:
        tbls = get_tables_from_sql_fluff(sql=sql, dialect=dialect)
    except (
        sqlfluff.core.errors.SQLParseError,
        sqlfluff.core.errors.SQLLexError,
        sqlfluff.api.simple.APIParsingError,
    ) as e:
        logging.warning(
            "SQLFluff parse failed; falling back to regex. SQL:\n%s", sql,
            exc_info=e
        )
        tbls = get_tables_from_sql_simple(sql)
    return list(tbls)


def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    """
    Read 'nodes' & 'sources' from dbt manifest for dashboard exposures.

    Filters by `dbt_db_name` if provided.

    Args:
        dbt_manifest (dict): Loaded JSON from target/manifest.json.
        dbt_db_name (str|None): If set, only tables in this database.

    Returns:
        dict[str, dict]: Mapping 'schema.table' → metadata:
          {
            'name': ..., 'schema': ..., 'database': ...,
            'type': 'node'|'source', 'ref': "ref(...)" or "source(...)"
          }
    """
    tables = {}
    for section in ("nodes", "sources"):
        for _, entry in dbt_manifest[section].items():
            name = entry["name"]
            schema = entry["schema"]
            database = entry["database"]
            source = entry["unique_id"].split(".")[-2]
            key = f"{schema}.{name}"

            if dbt_db_name is None or database == dbt_db_name:
                assert key not in tables, (
                    f"Table {key} duplicates across databases."
                )
                tables[key] = {
                    "name": name,
                    "schema": schema,
                    "database": database,
                    "type": section[:-1],
                    "ref": (
                        f"ref('{name}')"
                        if section == "nodes"
                        else f"source('{source}','{name}')"
                    ),
                }
    assert tables, "Manifest is empty!"
    return tables


def get_dashboards_from_superset(superset, superset_url, superset_db_id):
    """
    Fetch published dashboards and their "schema.table" datasets.

    Args:
        superset (Superset): Authenticated client.
        superset_url (str): Base URL (no /api/v1).
        superset_db_id (int|None): If set, filter datasets by DB ID.

    Returns:
        tuple[list[dict], set[str]]:
          - dashboards: each with 'id','title','url','owner_name','datasets'
          - set of all "schema.table" keys used.
    """
    logging.info("Getting published dashboards from Superset.")
    page = 0
    dash_ids = []
    while True:
        payload = {"q": json.dumps({"page": page, "page_size": 100})}
        res = superset.request("GET", "/dashboard/", params=payload)
        result = res["result"]
        if not result:
            break
        for d in result:
            if d.get("published"):
                dash_ids.append(d["id"])
        page += 1

    assert dash_ids, "No published dashboards found!"
    dashboards = []
    ds_w_db = set()
    for idx, did in enumerate(dash_ids, start=1):
        try:
            logging.info("Processing dashboard %d/%d.", idx, len(dash_ids))
            dash = superset.request("GET", f"/dashboard/{did}")["result"]
            owner = dash["owners"][0]
            owner_name = f"{owner['first_name']} {owner['last_name']}"
            url = f"{superset_url}/superset/dashboard/{did}"

            ds_list = superset.request("GET", f"/dashboard/{did}/datasets")["result"]
            parsed = [
                [d["database"]["name"], d["schema"], d["table_name"]]
                for d in ds_list
            ]
            # replace None → "None"
            parsed = [
                ["None" if p is None else p for p in trip]
                for trip in parsed
            ]
            with_db = [".".join(trip) for trip in parsed]
            wo_db   = [".".join(trip[1:]) for trip in parsed]
            ds_w_db.update(with_db)

            dashboards.append({
                "id": did,
                "title": dash["dashboard_title"],
                "url": url,
                "owner_name": owner_name,
                "datasets": wo_db,
            })
        except HTTPError as e:
            logging.error(
                "Failed to fetch dashboard %d info.", did, exc_info=e
            )

    # enforce schema.table uniqueness if no superset_db_id
    seen = set()
    for full in ds_w_db:
        key = ".".join(full.split(".")[1:])
        assert key not in seen or superset_db_id is not None, (
            f"Dataset {key} duplicates across DBs; "
            "set superset_db_id to disambiguate."
        )
        seen.add(key)

    return dashboards, seen


def get_datasets_from_superset(
    superset,
    dashboards_datasets,
    dbt_tables,
    sql_dialect,
    superset_db_id,
):
    """
    Fetch dataset details and map to dbt refs for exposures.

    Args:
        superset (Superset): Authenticated client.
        dashboards_datasets (set[str]): Keys "schema.table" to include.
        dbt_tables (dict): Output of get_tables_from_dbt().
        sql_dialect (str): SQLFluff dialect.
        superset_db_id (int|None): Filter by DB ID.

    Returns:
        dict[str, dict]: Mapping "schema.table" → metadata with:
          'kind','tables','dbt_refs', etc.
    """
    page = 0
    result = {}
    while True:
        payload = {"q": json.dumps({"page": page, "page_size": 100})}
        res = superset.request("GET", "/dataset/", params=payload)
        rows = res["result"]
        if not rows:
            break

        for r in rows:
            key = f"{r['schema']}.{r['table_name']}"
            dbid = r["database"]["id"]
            if key in dashboards_datasets and (
                superset_db_id is None or dbid == superset_db_id
            ):
                kind = r["kind"]
                if kind == "virtual":
                    tbls = get_tables_from_sql(r["sql"], sql_dialect)
                    tbls = [
                        t if "." in t else f"{r['schema']}.{t}"
                        for t in tbls
                    ]
                else:
                    tbls = [key]
                refs = [
                    dbt_tables[t]["ref"]
                    for t in tbls
                    if t in dbt_tables
                ]
                result[key] = {
                    "name": r["table_name"],
                    "schema": r["schema"],
                    "database": r["database"]["database_name"],
                    "kind": kind,
                    "tables": tbls,
                    "dbt_refs": refs,
                }
        page += 1

    return result


def merge_dashboards_with_datasets(dashboards, datasets):
    """
    Annotate each dashboard with the set of dbt refs it depends on.

    Args:
        dashboards (list[dict]): Output of get_dashboards_from_superset.
        datasets (dict): Output of get_datasets_from_superset.

    Returns:
        list[dict]: Each dashboard has added `refs` (sorted list[str]).
    """
    for dash in dashboards:
        refs = {
            ref
            for ds in dash["datasets"]
            if ds in datasets
            for ref in datasets[ds]["dbt_refs"]
        }
        dash["refs"] = sorted(refs)
    return dashboards


def get_exposures_dict(dashboards, exposures):
    """
    Merge existing exposures with newly harvested dashboards.

    Args:
        dashboards (list[dict]): Dashboards with `refs`.
        exposures (list[dict]): Previously-loaded exposures from YAML.

    Returns:
        list[dict]: New list of exposures ready for YAML dump.
    """
    dashboards.sort(key=lambda d: d["id"])
    titles = [d["title"] for d in dashboards]
    assert len(set(titles)) == len(titles), "Duplicate dashboard names!"
    orig = {e["url"]: e for e in exposures}

    result = []
    for d in dashboards:
        name = (
            re.sub(r"[^\w ]+", "", d["title"])
            .replace(" ", "_")
            .lower()
        )
        result.append({
            "name": name,
            "label": d["title"],
            "type": "dashboard",
            "url": d["url"],
            "description": orig.get(d["url"], {}).get("description", ""),
            "depends_on": d["refs"],
            "owner": {"name": d["owner_name"], "email": ""},
        })
    return result


class YamlFormatted(ruamel.yaml.YAML):
    """
    Custom ruamel.yaml YAML dumper with consistent formatting.
    """
    def __init__(self):
        super().__init__()  # Python 3 style
        self.default_flow_style = False
        self.allow_unicode = True
        self.encoding = "utf-8"
        self.block_seq_indent = 2
        self.indent = 4


def main(
    dbt_project_dir,
    exposures_path,
    dbt_db_name,
    *,
    superset_url,
    superset_db_id=None,
    sql_dialect,
    superset_access_token,
    superset_refresh_token,
):
    """
    Entry point: load manifest, pull dashboards, and write exposures YAML.

    Workflow:
      1. Load dbt manifest from
         <dbt_project_dir>/target/manifest.json.
      2. Load existing exposures YAML (if any).
      3. Crawl Superset dashboards & datasets.
      4. Merge with dbt refs and existing exposures.
      5. Dump updated exposures to `exposures_path`.

    Args:
        dbt_project_dir (str): dbt project root.
        exposures_path (str): Path to exposures YAML (within project).
        dbt_db_name (str|None): Filter manifest tables by this DB name.
        superset_url (str): Base Superset URL (no `/api/v1`).
        superset_db_id (int|None): Filter Superset by this DB ID.
        sql_dialect (str): SQLFluff dialect for parsing virtual tables.
        superset_access_token (str): Superset access token.
        superset_refresh_token (str): Superset refresh token.

    Raises:
        AssertionError: If authentication is missing.
    """
    assert superset_access_token or superset_refresh_token, (
        "Provide SUPERSET_ACCESS_TOKEN or SUPERSET_REFRESH_TOKEN."
    )

    client = Superset(
        f"{superset_url}/api/v1",
        access_token=superset_access_token,
        refresh_token=superset_refresh_token,
    )

    logging.info("Starting dashboard pull script.")
    manifest_file = f"{dbt_project_dir}/target/manifest.json"
    with open(manifest_file, encoding="utf-8") as f:
        dbt_manifest = json.load(f)

    yaml_path = dbt_project_dir + exposures_path
    try:
        with open(yaml_path, encoding="utf-8") as f:
            safe = ruamel.yaml.YAML(typ="safe")
            exposures = safe.load(f).get("exposures", [])
    except (FileNotFoundError, TypeError):
        Path(yaml_path).parent.mkdir(parents=True, exist_ok=True)
        Path(yaml_path).touch(exist_ok=True)
        exposures = []

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)
    dashboards, dash_ds = get_dashboards_from_superset(
        client, superset_url, superset_db_id
    )
    datasets = get_datasets_from_superset(
        client, dash_ds, dbt_tables, sql_dialect, superset_db_id
    )
    dashboards = merge_dashboards_with_datasets(dashboards, datasets)
    exposures_list = get_exposures_dict(dashboards, exposures)

    # Insert blank line before each exposure except the first
    seq = ruamel.yaml.comments.CommentedSeq(exposures_list)
    for idx in range(1, len(seq)):
        seq.yaml_set_comment_before_after_key(idx, before="\n")

    out = {"version": 2, "exposures": seq}
    dumper = YamlFormatted()
    with open(yaml_path, "w+", encoding="utf-8") as f:
        dumper.dump(out, f)

    logging.info("Wrote exposures to %s", yaml_path)
    logging.info("All done!")
