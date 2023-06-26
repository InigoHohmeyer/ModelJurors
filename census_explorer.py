#!/usr/bin/env python3
"""Downloader of full tables from census into JSONs.

Includes some tools for exploring available tables.

**Example**
Goal: Put all race population data for every place in every state in the data lake.

# Find the race table
tables = table_list()
descriptions = keys_to_dict(tables, 'description')
search_dict_keys(descriptions, 'RACE') # search for keyword within the descriptions
RACE in 'descriptions'

# Look at the outptut above to fine the appropriate table, in this case B02001

# Download the race table, format it, upload it
vintage = {"year": 2020, "dataset": "acs", "estimate": "acs5"}
huge_table = table_fetch(table='B02001', vintage=vintage, geography='place')
jsondir, jsonfile = datavault_json(huge_table)
import upload_blob from missing_airtable_alerts  # local module
upload_blob(jsondir + jsonfile, jsondir)
import data_lake as dl
sqlcode = generate_sql(huge_table)
dl.execute_on_snowflake(sql=sqlcode)


TODO: merge generate_sql() with data_lake version
"""

from distutils.log import warn
import os
import re
import string
from types import MappingProxyType  # to prevent dangerous (default) value warnings
import json
import pandas as pd
import numpy as np
from dotenv import load_dotenv

import us
import requests

current_dir = os.getcwd() + "/"
if os.path.split(os.getcwd())[-1] in ["automations", "prototyping"]:
    repo_dir = current_dir + "../"
else:
    repo_dir = current_dir

load_dotenv(dotenv_path=repo_dir + ".env")

locality_vars_default = (
    "P2_001N",
    "P2_002N",
    "P2_003N",
    "P2_004N",
    "P2_005N",
    "P2_006N",
    "P2_007N",
    "P2_008N",
    "P2_009N",
    "P2_010N",
)
url_base = "https://api.census.gov/data/"

# makes this dict immutable
vintage_default = MappingProxyType({"year": 2020, "dataset": "acs", "estimate": "acs5"})
vintage_decennial = MappingProxyType({"year": 2020, "dataset": "dec", "estimate": "pl"})

## TESTING
def main():
    """main function for testing

    runs
    huge_table = table_fetch(table="B02001", geography=["state"])

    Returns
    -------
    huge_table : dict
    """
    # huge_table = table_fetch(table="B02001", geography=["place"])
    # huge_table = table_fetch(
    #     table="S1701", vintage=vintage_default, geography=["county"]
    # )
    huge_table = table_fetch(
        table="P159", vintage=vintage_default, geography=["county"]
    )
    return huge_table


def table_list(
    vintage: dict | MappingProxyType = vintage_default,
    main_tables_only: bool = True,
) -> dict:
    """Fetches names and descriptions of all "groups" (tables).

    Parameters
    ----------
    vintage : dict, optional
        DEFAULT: {"year": 2020, "dataset": "acs", "estimate": "acs5"}

    main_tables_only : bool, optional
        DEFAULT: True
        Cuts out tables pre-sliced by other variables, eg a particular race.

    Returns
    -------
    dict
        _description_
    """
    url = (
        f"{url_base}{vintage['year']}/{vintage['dataset']}/"
        f"{vintage['estimate']}/groups.json"
    )
    response = requests.get(url)
    groups = json.loads(response.content)["groups"]

    if main_tables_only:
        # Cuts the final letter (7th position of group name)
        return {
            g["name"]: {k: v for k, v in g.items() if k != "name"}
            for g in groups
            if len(g["name"]) == 6
        }
    else:
        return {g["name"]: {k: v for k, v in g.items() if k != "name"} for g in groups}


def table_exists(
    table: str | list, vintage: dict | MappingProxyType, raise_error: bool = False
) -> bool | list:
    """Check if a table exists.

    Parameters
    ----------
    table : str | list
        Name of table to check
    vintage : dict | MappingProxyType
    raise_error : bool, optional
        DEFAULT: False

    Returns
    -------
    bool | list
        True if table exists, False if not.
        If table is a list, returns a list of booleans.

    Raises
    ------
    ValueError
        If table doesn't exist
    """
    if isinstance(table, list):
        return [
            table_exists(table=t, vintage=vintage, raise_error=raise_error)
            for t in table
        ]

    url = (
        f"{url_base}{vintage['year']}/{vintage['dataset']}/"
        f"{vintage['estimate']}{'/subject' if table[0]=='S' else ''}/groups/"
        f"{table}.json"
    )
    # print(url)
    response = requests.get(url)
    if response.status_code == 200:
        return True
    else:
        if raise_error:
            raise ValueError(
                f"Table {table} does not exist for this vintage\n:"
                f"{vintage['year']}/{vintage['dataset']}/{vintage['estimate']}"
            )
        else:
            return False


def variable_info(
    variable: str,
    vintage: dict | MappingProxyType = vintage_default,
) -> dict:
    """Fetch information about a particular variable.

    Parameters
    ----------
    variable : str
        Name of variable to get information about
    vintage : dict | MappingProxyType, optional
        DEFAULT: {"year": 2020, "dataset": "acs", "estimate": "acs5"}

    Returns
    -------
    dict
    """
    url = (
        f"{url_base}{vintage['year']}/{vintage['dataset']}/"
        f"{vintage['estimate']}/variables/{variable}.json"
    )
    response = requests.get(url)

    return json.loads(response.content)


def variable_list(
    table: dict | str | list,
    estimates_only: bool = True,
    vintage: dict | MappingProxyType = vintage_default,
) -> dict:
    """Fetch all variables (or just the estimates) for a particular table.

    Parameters
    ----------
    table : dict | str | list
        The name of the table or dict output of table_list()
    estimates_only : bool, optional
        DEFAULT: True
    vintage : dict | MappingProxyType, optional
        DEFAULT: {"year": 2020, "dataset": "acs", "estimate": "acs5"}

    Returns
    -------
    dict
    """
    if isinstance(table, list):
        variables = {}
        for t in table:
            variables.update(
                variable_list(table=t, estimates_only=estimates_only, vintage=vintage)
            )
        return variables

    table_exists(table=table, vintage=vintage, raise_error=True)

    if isinstance(table, dict):
        url = table["variables"]
    elif isinstance(table, str) and vintage is not None:
        url = (
            f"{url_base}{vintage['year']}/{vintage['dataset']}/"
            f"{vintage['estimate']}{'/subject' if table[0]=='S' else ''}/groups/"
            f"{table}.json"
        )

    # print(url)  # debug
    response = requests.get(url)
    variables = json.loads(response.content)["variables"]
    if estimates_only:
        variables = {
            k: v
            for k, v in variables.items()
            if k[-1] != "A" and k[-1] != "M"  # and k[-1] != "P"
        }

    # Sort by key
    return {k: variables[k] for k in list(sorted(variables.keys()))}


def variable_names_to_info(
    variables: list | tuple,
    vintage: dict | MappingProxyType = vintage_default,
) -> dict:
    """Fetch information about a list of variables.

    Parameters
    ----------
    variables : list | tuple
        List of variable names
    vintage : dict | MappingProxyType, optional
        DEFAULT: {"year": 2020, "dataset": "acs", "estimate": "acs5"}

    Returns
    -------
    dict
    """
    # If table_variables is a list of strings, find the table(s) and
    # load the variable list(s)
    if (
        isinstance(variables, list)
        or isinstance(variables, tuple)
        and isinstance(variables[0], str)
    ):
        tables = list({v.split("_")[0] for v in variables})
        new_table_variables = {}
        for t in tables:
            new_table_variables.update(variable_list(table=t, vintage=vintage))
        return {key: new_table_variables[key] for key in variables}
    else:
        return variables


def variables_to_column_names(
    table_variables: list | dict,
    vintage: dict = vintage_default,
    rename_dict: bool = False,
) -> list | dict:
    """One-liner to reformat the column names to something more SQL-friendly

    Parameters
    ----------
    table_variables : list
        This is a list of dicts output from variables_list

    Returns
    -------
    list
    """
    colnames = []

    table_variables = variable_names_to_info(variables=table_variables, vintage=vintage)

    for k, v in sorted(table_variables.items()):
        if "universe" not in v.keys():
            # v["universe"] = ""
            v["universe"] = v["concept"].split(" ")[0]
        colnames.append(
            f'{v["universe"]}_{"_".join(v["label"].split("!!")[1:]).replace("-", "_")}'
        )
        table_variables[k]["rename"] = colnames[-1]

    # apply valid_sql_col_name() to each colname
    if rename_dict:
        return {
            k: valid_sql_col_name(v["rename"], lowercase=False)
            for k, v in table_variables.items()
        }
    else:
        return [valid_sql_col_name(c, lowercase=False) for c in colnames]


# Test queries
# https://api.census.gov/data/2020/acs/acs5?get=group(B02001)&for=state:36&for=place:51000
# https://api.census.gov/data/2020/acs/acs5?get=NAME,B02001_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B02001_006E,B02001_007E,B02001_008E,B02001_009E,B02001_010E&for=state:*&for=place:51000
# https://api.census.gov/data/2020/acs/acs5/subject?get=NAME,S1701_C01_001E,S1701_C01_002E,S1701_C01_003E,S1701_C01_004E,S1701_C01_005E,S1701_C01_006E,S1701_C01_007E,S1701_C01_008E,S1701_C01_009E,S1701_C01_010E,S1701_C01_011E,S1701_C01_012E,S1701_C01_013E,S1701_C01_014E,S1701_C01_015E,S1701_C01_016E,S1701_C01_017E,S1701_C01_018E,S1701_C01_019E,S1701_C01_020E,S1701_C01_021E,S1701_C01_022E,S1701_C01_023E,S1701_C01_024E,S1701_C01_025E,S1701_C01_026E,S1701_C01_027E,S1701_C01_028E,S1701_C01_029E,S1701_C01_030E,S1701_C01_031E,S1701_C01_032E,S1701_C01_033E,S1701_C01_034E,S1701_C01_035E,S1701_C01_036E,S1701_C01_037E,S1701_C01_038E,S1701_C01_039E,S1701_C01_040E,S1701_C01_041E,S1701_C01_042E,S1701_C01_043E,S1701_C01_044E,S1701_C01_045E,S1701_C01_046E,S1701_C01_047E,S1701_C01_048E,S1701_C01_049E&for=county&in=state:36


def parse_geography_for_url(geography: str | list | int, out_string: str = "") -> str:
    """Parse geography for use in a Census API URL.

    Parameters
    ----------
    geography : str | list | int
        _description_
    out_string : str, optional
        _description_, by default ""

    Returns
    -------
    str
        _description_

    Raises
    ------
    ValueError
        _description_
    """

    # https://api.census.gov/data/2020/dec/pl?get=NAME&for=block:*&in=state:06%20county:037
    # https://api.census.gov/data/2020/dec/pl?get=NAME,P2_001N&for=block&in=state:36%20county:047%20tract:11000
    # https://api.census.gov/data/2020/dec/pl?get=NAME,P2_001N&for=tract&in=state:01,32,36

    # fix congressional district geography name
    cd = lambda x: re.sub(r"congressional[ _]district", "congressional%20district", x)

    def set_level(out_string: str = "") -> str:
        if out_string == "":
            return "&for="
        elif "&in=" in out_string:
            return "%20"
        elif "&for=" in out_string:
            return "&in="
        else:
            return "%20"

    # if isinstance(geography, list) and len(geography) == 1:
    #     geography = geography[0]

    # if geography is an int or a string that only contains numeric
    # characters, commas, and/or asterisks, assume it's FIPS code(s)
    if isinstance(geography, int) or (
        isinstance(geography, str) and re.match(r"^[\d,*]+$", geography)
    ):
        return f"{out_string}:{geography}"
    elif isinstance(geography, str):
        # print(out_string, set_level(out_string)) # debug
        return f"{out_string}{set_level(out_string)}{cd(geography)}"
    elif isinstance(geography, list):
        if all(list(map(lambda x: type(x) == int, geography))) or all(
            list(map(lambda x: type(x) == str, geography))
        ):
            return parse_geography_for_url(",".join(geography), out_string=out_string)

        for g in geography:
            out_string = parse_geography_for_url(g, out_string=out_string)

        return out_string


def census_url(
    geography: str | list, vintage: dict | MappingProxyType, varnames: str
) -> str:
    """Generate the URL for the Census API.

    Parameters
    ----------
    geography : str
    vintage : dict | MappingProxyType
    variables : list
        List of variables to fetch/
        See variable_list() for more info

    Returns
    -------
    str
        URL for the Census API.
    """

    if isinstance(geography, str) and geography == "tract":
        warn("fetching all tracts in all states! this may take a while...")
        fipses = [s.fips for s in us.states.STATES + [us.states.DC]]
        geography = [geography, ["state", fipses]]

    geos = parse_geography_for_url(geography=geography)
    url = (
        f"{url_base}{vintage['year']}/{vintage['dataset']}/"
        f"{vintage['estimate']}{'/subject' if varnames[0][0] == 'S' else ''}"
        f"?get=NAME,{ varnames}{geos}"
    )
    print(url)  # debug

    return url


def fetch_data(url: str) -> dict:
    """Fetch the data from the Census API.

    Parameters
    ----------
    url : str
        URL for the Census API.

    Returns
    -------
    dict
        JSON-friendly dict of the data.
    """
    response = requests.get(url)
    try:
        return json.loads(response.content)
    except json.decoder.JSONDecodeError:
        return response


def table_fetch(
    table: str | list,
    geography: list | str,
    vintage: dict | MappingProxyType = vintage_default,
    estimates_only: bool = True,
    variables: list | dict | tuple = None,
    include_subtables: bool = True,
) -> dict:
    """Fetches a whole table, with the full geography.
    Outputs to a JSON-friendly dict, for uploading to data lake.

    Parameters
    ----------
    table : str | list
        The name of the table.
        To find this, see:
            table_list()
            keys_to_dict()
            search_keys()
        This can also be a list of tables.
        Adding a wildcard (*) to the end of the table name will fetch all subtables,
        e.g. "B19013*" will fetch and merge B19013, B19013A, B19013B, etc.
    geography : list | str
        Options:
            'state'
            'place'
            'county'
            'congressional_district'
            'congressional district'
            'congressional%20district' # this is the true (and dumb) API variable
            ['tract', ['state', 36]] # tract requires state specification
            ['tract', ['state', 36], ['county', '*']] # same as above
            ['tract', ['state', 36], 'county'] # same as above
            ['tract', ['state', 36], ['county', 47]] # tracts in Brooklyn
            ['place', ['state', 36]] # places in state=36 (NY)
            ['county', ['state', 36]] # county in state=36 (NY)
            ...
            ['block', ['state', 36], ['county', 47], ['tract', 11000]] # block in tract 11000 in county 47 in state 36 (NY)
            TOFILL
            ...
    vintage : dict | MappingProxyType, optional
        DEFAULT: {"year": 2020, "dataset": "acs", "estimate": "acs5"}
    estimates_only : bool, optional
        If True, only fetches estimates.
        DEFAULT: True
    variables : list | dict | tuple, optional
        List of variables to fetch.
    include_subtables : bool, optional
        If True, will fetch all subtables of the table.
        DEFAULT: True

    Returns
    -------
    huge_table: dict

    TODO: Fix default of trying 10 subtables on wildcard input
    """
    if include_subtables:
        # append '*' to table name to fetch all subtables
        if isinstance(table, str) and "*" not in table:
            table = f"{table}*"

    # If wildcard table name input, recurse
    if isinstance(table, str) and table[-1] == "*":
        tables = [
            f"{table[:-1]}{lett}" for lett in [""] + list(string.ascii_uppercase[:9])
        ]
        for i, t in enumerate(tables):
            if not table_exists(table=t, vintage=vintage, raise_error=False):
                tables = tables[:i]
                break
        if tables == []:
            raise ValueError(
                f"Table {t} {vintage['year']}"
                f"/{vintage['dataset']}/{vintage['estimate']} "
                f"does not exist."
            )
        everything = table_fetch(
            table=tables,
            geography=geography,
            vintage=vintage,
            estimates_only=estimates_only,
            variables=variables,
            include_subtables=False,
        )
        # print(everything)  # debug
        if i > 1:
            everything["table_name"] = table[:-1] + "_" + string.ascii_uppercase[:i]
        # print(f'Output table name: {everything["table_name"]}')
        return everything

    table_exists(table=table, vintage=vintage, raise_error=True)

    # Get full information on variables, so we can get the column names
    if variables is None:
        variables = variable_list(
            table=table, estimates_only=estimates_only, vintage=vintage
        )
    elif isinstance(variables, list) or isinstance(variables, tuple):
        variables = variable_names_to_info(variables=variables, vintage=vintage)

    # print(list(variables.keys()))  # debug
    # print(f"Loading tables: {table}")  # debug

    # Recursively run if there are too many variables (census API limit is 50)
    if len(list(variables.keys())) > 49:
        warn("too many variables (max 50). splitting into multiple requests...")

        everythings = []
        for i in range(0, len(list(variables.keys())), 49):
            # print(table)  # [0]) # debug
            everythings.append(
                table_fetch(
                    table=table,
                    geography=geography,
                    vintage=vintage,
                    estimates_only=estimates_only,
                    variables={k: variables[k] for k in list(variables)[i : i + 49]},
                )
            )
        return merge_fetched_census_tables(everythings)

    column_names = variables_to_column_names(variables, vintage=vintage)
    varnames = ",".join(variables)

    url = census_url(geography=geography, vintage=vintage, varnames=varnames)
    data = fetch_data(url)

    if isinstance(table, list):
        table_name = "_".join(table)
    else:
        table_name = table

    # put it all together
    everything = {
        "table_name": table_name,
        "vintage": vintage,
        "geography": geography,
        "variables": variables,
        "column_names": column_names,
        "header": data[0],
        "data": replace_666(data[1:]),
    }

    return everything


def replace_666(data: list) -> list:
    """Replace all entries equal to '-666666666' in list of lists with None

    Parameters
    ----------
    data : list
        Census data with nasty -666666666 entries

    Returns
    -------
    data: list
        Cleaned Census data
    """
    for i, row in enumerate(data):
        for j, col in enumerate(row):
            if col in ["-666666666", "-222222222", "-333333333"]:
                data[i][j] = None
    return data


def merge_fetched_census_tables(all_tables: list) -> dict:
    """Merges fetched tables into one big table.

    Parameters
    ----------
    all_tables : list
        A list of dicts, each one being a table.

    Returns
    -------
    dict

    """
    # Unique table names in all_tables
    ntables = len([t["table_name"] for t in all_tables])

    everything = {}

    for t in all_tables:
        ngeos = len(t["geography"])
        for k, v in t.items():
            if k not in everything:
                everything[k] = v
            elif ntables > 1:  # merge different tables, same variables
                if k == "data":
                    everything[k].extend(v)
                elif k == "table_name":
                    everything[k] += "_" + v
                elif k == "column_names":
                    everything[k].extend(v)
                else:
                    pass
            else:  # merge same table, different variables
                if k == "table_name":
                    if everything[k] != v:
                        everything[k] += "_" + v
                elif k in ["vintage", "geography"]:
                    if everything[k] != v:
                        raise ValueError(f"{k} don't match")
                elif k == "variables":
                    for vkeys in v:
                        if vkeys not in everything[k]:
                            everything[k][vkeys] = v[vkeys]
                elif k == "column_names":
                    everything[k].extend(v)
                elif k == "header":
                    everything[k].extend(v[1:-ngeos])
                elif k == "data":
                    # merge data horizontally
                    everything[k] = [
                        row1 + row2[1:-ngeos] for row1, row2 in zip(everything[k], v)
                    ]
                    # TODO: check that these line up correctly
                else:
                    pass

    # unit tests (these only work if merging from the same table)
    # if there is no underscore, then there is only one table
    if ntables == 1:
        ngeos = len(everything["geography"])
        nvars = len(everything["variables"])
        nrows = len(everything["data"])
        ncols = sum([len(t["data"][0]) - ngeos - 1 for t in all_tables]) + ngeos + 1

        assert len(everything["column_names"]) == nvars
        # TODO: debug this. where do the extra geographies come from? functionize.
        assert (
            len(np.unique(everything["header"])) == nvars + ngeos + 1  # (+1 for name)
            or len(np.unique(everything["header"]))
            == nvars
            + ngeos
            + 1  # (+1 for name)
            + [1 if "county" in everything["geography"] else 0][0]
            + [1 if "place" in everything["geography"] else 0][0]
            + [1 if "tract" in everything["geography"] else 0][0]
        )
        for t in all_tables:
            assert len(t["data"]) == nrows
        assert len(everything["data"][0]) == ncols
        assert len(everything["data"][-1]) == ncols
    # TODO: tests for when merging from different tables
    return everything


def datavault_json(fetched_table: dict) -> tuple[str, str]:
    """Prepare data for data lake ingestion:
        1. dump to a json
        2. format the blob path

    Parameters
    ----------
    fetched_table : dict
        The table data fetched using table_fetch()

    Returns
    -------
    jsondir : str
    jsonfile : str
        Names of the json file dumped and the blob path
    """

    if isinstance(fetched_table["geography"], str):
        geography = fetched_table["geography"]
    elif isinstance(fetched_table["geography"], list):
        geography = fetched_table["geography"][0]

    dataset = (
        f"{fetched_table['vintage']['dataset']}"
        f"_{fetched_table['vintage']['estimate']}"
        f"/{fetched_table['vintage']['year']}/{geography}"
    )
    provider = "census"

    jsondir = f"{provider}/{dataset}/"
    jsonfile = fetched_table["table_name"] + ".json"

    if not os.path.exists(jsondir):
        os.makedirs(jsondir)
        # print("%s jsondir did not exist. It does now." % jsondir)

    # Dump airtable data to JSON
    with open(jsondir + jsonfile, "w", encoding="utf-8") as outfile:
        json.dump(fetched_table["data"], outfile)

    return jsondir, jsonfile


def valid_sql_col_name(text: str, lowercase: bool = True) -> str:
    """Turn a string into a valid SQL column name by:
    1. replacing spaces with underscores,
    2. removing any non-alphanumeric, non-underscore characters, and
    3. prepending an underscore if the first character is numeric.

    Parameters
    ----------
    text : str
    lowercase : bool, optional
        DEFAULT: True

    Returns
    -------
    str
        Valid SQL column name.
    """
    # Replace spaces with underscores,
    # then remove any non-alphanumeric, non-underscore characters
    if lowercase:
        text = text.lower()
    text = re.sub("[^a-zA-Z0-9_-]", "", text.replace(" ", "_"))

    # If the first character is a number, prepend an underscore
    if re.match(r"^\d", text):
        text = f"_{text}"

    return text


def generate_sql(
    everything: dict | str, create_view: bool = True, file_path: str = repo_dir
) -> str:
    """Generate SQL from a fetched table.

    Parameters
    ----------
    everything : dict | str
        _description_
    create_view : bool, optional
        DEFAULT: True
    file_path : str, optional
        DEFAULT: repo_dir

    Returns
    -------
    str
        _description_
    """
    # TODO: escape more characters in the variable name list

    code = "use role engineer;\nuse warehouse etl;\n\nuse schema raw;\n\n"

    jsondir, jsonfile = datavault_json(everything)

    sql_table_name = f'{jsondir.replace("/","-")}{jsonfile[:-5]}'

    code += f"create or replace external table "
    code += f'"{sql_table_name}"\n(\n'

    # create view code
    view_code = f'create or replace view "{sql_table_name}-named" as\nselect\n'

    for i, f in enumerate(everything["header"]):
        if f in everything["variables"]:
            datatype = everything["variables"][f]["predicateType"]
            view_code += f"    {f.lower()} as {everything['column_names'][i-1]},\n"
        elif f.lower() == "name":
            datatype = "varchar"
            view_code += "    name,\n"
        else:
            datatype = "varchar"
            view_code += f"    {f.lower()},\n"
        code += f"    {f.lower()} {datatype} as (value[{i}]::{datatype}),\n"

    if f.lower() == "tract":
        code += f"    geoid varchar as (concat(value[{i-2}],value[{i-1}],value[{i}])::varchar)\n"
    else:
        code += f"    geoid varchar as (concat(value[{i-1}],value[{i}])::varchar)\n"
    # code += f")\nwith location = @raw.manual_storage/{jsondir}\n"
    code += f")\nwith location = @raw.manual_storage/{jsondir}\npattern='.*{jsonfile[:-5]}.*'\n"
    code += 'file_format = raw."census";'

    if create_view:
        view_code += f'    geoid\nfrom "{sql_table_name}";'
        code += f"\n\n{view_code}"

    # Write to file
    if file_path is not None:
        filename = f"{file_path}raw.{sql_table_name}.sql"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as outfile:
            outfile.write(code)
        print(f"Wrote sql code to {filename}")

    return code


# Helper functions for searching
def keys_to_dict(given_dict: dict, query: str) -> dict:
    """Rearranges a dicts into a dictionary keyed by a specified key
    within the dicts listed. Useful for searching list of tables.

    For example:

    tables = table_list()
    universes = keys_to_dict(tables, 'universe ')
    descriptions = keys_to_dict(tables, 'description')

    search_keys(descriptions, 'RACE') # search for keyword within the descriptions
    'RACE' in descriptions            # search for a description exactly matching

    Parameters
    ----------
    given_dict : dict
    key : str
        Key in dicts listed to become the key of the new dictionary.

    Returns
    -------
    dict
    """
    newdict = {}
    for gk, gv in given_dict.items():
        # Fix for trailing spaces in field names (census data is dumb)
        if query not in list(gv.keys()) and f"{query} " in list(gv.keys()):
            query = f"{query} "

        if query in list(gv.keys()):
            if gv[query] not in newdict:
                newdict[gv[query]] = list()
            # g_excl_key = {k: v for k, v in gv.items() if k != query}
            newdict[gv[query]].append(gk)

    return newdict


def search_list_substring(list_of_strings: list, query: str) -> list:
    """Make a list of search hits on keys of a dict.

    Parameters
    ----------
    list_of_strings : list
    query : str

    Returns
    -------
    list
    """
    matches = []

    for k in list_of_strings:
        if k.lower().find(query.lower()) >= 0:
            matches.append(k)
    return matches


def search_dict_keys(dict_w_keys: dict, query: str) -> list:
    """One-liner to pass a dict's keys to a search for a substring

    Parameters
    ----------
    dict_w_keys : dict
    query : str

    Returns
    -------
    list
        _description_
    """
    return search_list_substring(list_of_strings=list(dict_w_keys.keys()), query=query)


def get_all_locality_names(
    locality: str = "place",
    vintage: dict | MappingProxyType = vintage_decennial,
    variables: list | tuple | str = locality_vars_default,
    table: str = "P2",
) -> pd.DataFrame:
    """Download a table for all places in the US.

    Note: This contains lots of gnarly fixes for place names, to strip them
    down to their most essential elements, while still preserving as much as
    possible about their locality type (city, town, etc.).
    This is a bit of a hack, but it works. [<-- copilot suggested this, ha]

    Parameters
    ----------
    locality : str, optional
        Locality type to download. Options are "place", "county"
        DEFAULT: "place"
    vintage : dict | MappingProxyType, optional
        DEFAULT: {"year": 2020, "dataset": "dec", "estimate": "pl"}
    variables : list | tuple, optional
        Variable(s) to download.
        DEFAULT: "P2_001N" (the population)
    table : str, optional
        Table to download.
        DEFAULT: "P2"

    Returns
    -------
    huge_table: dict
        Same format as table_fetch() output
    """

    if isinstance(variables, str):
        variables = [variables]

    huge_table = table_fetch(
        table=table, vintage=vintage, geography=locality, variables=variables
    )

    df = pd.DataFrame(huge_table["data"], columns=huge_table["header"])

    # Drop PR
    df = df[df["state"] != us.states.PR.fips]

    # various fixes
    df = fix_locality(df=df, locality=locality)

    for v in variables:
        df[v] = df[v].copy().astype(int)
    df.sort_values(["state", locality], inplace=True)

    # reorder for proper geoid construction
    newcols = df.columns.tolist()
    newcols.remove("state")
    newcols.remove(locality)
    newcols = newcols + ["state", locality]

    huge_table["data"] = df[newcols].values.tolist()
    huge_table["header"] = df[newcols].columns.values.tolist()

    return huge_table  # , df


def fix_locality(df: pd.DataFrame, locality: str = "place") -> pd.DataFrame:
    """_summary_

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    locality : str, optional
        options: 'place', 'county'
        DEFAULT: 'place'

    Returns
    -------
    pd.DataFrame
        _description_
    """

    # GENERAL HELPERS
    def separate_state_names(df: pd.DataFrame) -> pd.DataFrame:
        df["state_name"] = df["NAME"].str.split(",").str[1].str.strip()
        df["NAME"] = df["NAME"].str.split(",").str[0]  # strip state at end
        return df

    # PLACE HELPERS
    def fix_duplicate_place_names(df: pd.DataFrame) -> pd.DataFrame:
        df["place_type"] = df["NAME"].str.split(" ").str[-1]
        df["NAME"] = df["NAME"].str.split(" ").str[:-1].apply(lambda x: " ".join(x))
        # there are some CDPs with the same name, but different counties
        types_of_name_errors = ["County)", "Counties)"]  # , 'Municipio)']
        for ne in types_of_name_errors:
            df["tmp_place_type"] = (
                df["NAME"][df["place_type"] == ne]
                .str.split(" \(")
                .str[0]
                .str.split(" ")
                .str[-1]
            )

            df["NAME"][df["place_type"] == ne] = df[["NAME", "place_type"]][
                df["place_type"] == ne
            ].apply(
                lambda x: " ".join(x), axis=1
            )  # .str.replace('CDP ','')
            # remove the text in the tmp_place_type column from the name
            df["NAME"][df["place_type"] == ne] = df[df["place_type"] == ne].apply(
                lambda x: x["NAME"].replace(x["tmp_place_type"] + " ", ""), axis=1
            )
            df["place_type"][df["place_type"] == ne] = df["tmp_place_type"][
                df["place_type"] == ne
            ]
        df["tmp_place_type"][df["NAME"] == ""] = df["place_type"][df["NAME"] == ""]
        df["place_type"][df["NAME"] == ""] = ""  # drop this row
        df["place_type"] = df["place_type"].str.lower()
        df["NAME"][df["NAME"] == ""] = df["tmp_place_type"][df["NAME"] == ""]
        return df

    def fix_place_types(df: pd.DataFrame) -> pd.DataFrame:
        # deal with ... government (balance)
        # deal with ... city (balance)
        types_of_name_errors = ["government", "city"]
        for ne in types_of_name_errors:
            mask = df["NAME"].str.contains(ne)
            df["place_type"][mask] = (
                df["NAME"][mask].str.split(" ").str[-2]
                + f" {ne} "
                + df[mask]["place_type"]
            )
            # actually, just keep the first word
            df["NAME"][mask] = df["NAME"][mask].str.split("[ -/]").str[0]

        # deal with county governments
        mask = df["place_type"] == "government"
        df["tmp_place_type"][mask] = (
            df["NAME"][mask].str.split(" ").str[-2:].str.join(" ")
        )
        df["NAME"][mask] = df["NAME"][mask].str.split(" ").str[:-2].str.join(" ")
        df["place_type"][mask] = (
            df["tmp_place_type"][mask] + " " + df["place_type"][mask]
        )

        # clean up place_type
        df["place_type"] = df["place_type"].astype(str)
        mask = df.apply(
            lambda x: x["NAME"].split(" ")[0] == x["place_type"].split(" ")[0], axis=1
        )
        df.loc[mask, "place_type"] = (
            df[mask]["place_type"].str.split(" ").str[1:].str.join(" ")
        )
        return df

    def fix_state_name_errors(df: pd.DataFrame) -> pd.DataFrame:
        state_names_valid = [x.name for x in us.STATES + [us.states.DC]]
        for state in df["state_name"].unique():
            if state not in state_names_valid:
                mask = df["state_name"] == state
                df.loc[mask, "place_type"] = (
                    df[mask]["state_name"].str.split(" ").str[-1]
                )
                df.loc[mask, "NAME"] = (
                    df[mask]["NAME"]
                    + " "
                    + df[mask]["state_name"].str.split(" ").str[:-1].str.join(" ")
                )
                df.loc[mask, "state_name"] = df["state_name"][
                    df["state"] == df[mask]["state"].unique()[0]
                ].mode()[0]
        return df

    # def extract_counties(df: pd.DataFrame) -> pd.DataFrame:
    #     df["county_name"] = df["NAME"].str.split(" ").str[-1]
    #     df["NAME"] = df["NAME"].str.split(" ").str[:-1].str.join(" ")
    #     return df

    def separate_county_from_name(df: pd.DataFrame) -> pd.DataFrame:
        mask = df["NAME"].str.contains(r"Count[yies]")
        df.loc[mask, "county_name"] = df["NAME"][mask].str.extract(
            r"\(([A-z\s']+)\sCount[yies]"
        )[0]
        df.loc[mask, "NAME"] = df["NAME"][mask].str.split("(").str[0]
        return df

    ### COUNTY HELPERS
    def county_splitter(df):
        county_types = ["county", "city", "parish", "area", "municipality", "borough"]
        this_type = str.lower(df["NAME"].split()[-1])
        if this_type in county_types:
            return " ".join(df["NAME"].split()[:-1]), this_type
        elif df["NAME"] == "District of Columbia":
            return df["NAME"], "district"
        else:
            return df["NAME"], None

    # MAIN FUNCTION
    df = separate_state_names(df)

    if locality == "place":
        df = fix_duplicate_place_names(df)
        df = fix_place_types(df)
        df = fix_state_name_errors(df)
        df = separate_county_from_name(df)
        # one-off fix
        df["place_type"][df["NAME"] == "Felicity"] = "village"

        df.drop("tmp_place_type", axis=1, inplace=True)
    elif locality == "county":
        result = df.apply(county_splitter, axis=1)
        result = pd.DataFrame.from_dict(
            dict(result), orient="index", columns=["NAME", "county_type"]
        )

        df["NAME"] = result["NAME"]
        df["county_type"] = result["county_type"]

    return df


def to_pandas(huge_table: dict, readable_columns: bool = False) -> pd.DataFrame:
    """Converts a "huge_table" output to a pandas dataframe"""
    df = pd.DataFrame(huge_table["data"], columns=huge_table["header"])

    if readable_columns:
        rename_cols = variables_to_column_names(
            huge_table["variables"], huge_table["vintage"], rename_dict=True
        )
        df.rename(columns=rename_cols, inplace=True)

    return df


# Run the main() function if calling this file from the command line
if __name__ == "__main__":
    main()
