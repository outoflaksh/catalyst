from sqlalchemy import create_engine
from pandas import read_excel, read_csv, DataFrame


def generate_creation_query(schema: dict, table_name: str, pk_col: str):
    conversion_scheme = {"int64": "BIGINT", "object": "TEXT", "float64": "NUMERIC"}

    creation_query = []

    for c in schema:
        constraint = "PRIMARY KEY" if pk_col == c else ""
        sanitized_col = c.replace(" ", "_").replace("-", "")
        creation_query.append(
            f"{sanitized_col} {conversion_scheme[schema[c]]} {constraint}"
        )

    creation_query = (
        f"CREATE TABLE IF NOT EXISTS {table_name} (" + ",".join(creation_query) + ");"
    )

    return creation_query


def generate_insertion_queries(dataframe: DataFrame, schema: dict, table_name: str):
    insertion_queries = []

    for i in range(len(dataframe)):
        insertion_query = []
        for v in schema:
            insertion_query.append(f"'{dataframe.loc[i, v]}'")

        insertion_query = (
            f"INSERT INTO {table_name} VALUES(" + ",".join(insertion_query) + ");"
        )

        insertion_queries.append(insertion_query)

    return insertion_queries


def excel_to_sql(sql_uri: str, excel_file_path: str, table_name: str, pk: str = None):
    engine = create_engine(url=sql_uri, echo=True)

    df = read_excel(excel_file_path)

    excel_schema = dict(df.dtypes)
    excel_schema = {i: str(excel_schema[i]) for i in excel_schema}

    creation_query = generate_creation_query(
        schema=excel_schema, table_name=table_name, pk_col=pk
    )

    conn = engine.connect()
    conn.execute(creation_query)

    insertion_queries = generate_insertion_queries(df, excel_schema, table_name)

    for insertion_query in insertion_queries:
        try:
            conn.execute(insertion_query)
        except Exception as err:
            print(err)
        finally:
            continue


def csv_to_sql(sql_uri: str, csv_file_path: str, table_name: str, pk: str = None):
    engine = create_engine(url=sql_uri, echo=True)

    df = read_csv(csv_file_path)

    csv_schema = dict(df.dtypes)
    csv_schema = {i: str(csv_schema[i]) for i in csv_schema}

    creation_query = generate_creation_query(
        schema=csv_schema, table_name=table_name, pk_col=pk
    )

    conn = engine.connect()
    conn.execute(creation_query)

    insertion_queries = generate_insertion_queries(df, csv_schema, table_name)

    for insertion_query in insertion_queries:
        try:
            conn.execute(insertion_query)
        except Exception as err:
            print(err)
        finally:
            continue
