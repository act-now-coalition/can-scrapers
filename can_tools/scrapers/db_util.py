import io
import textwrap

import numpy as np
import pandas as pd
import sqlalchemy as sa


def fast_to_sql(
        df, conn, name, index=False, if_exists="append",
        cols=None, schema=None, temp=False
    ):
    """
    Function used to do fast inserts into SQL.
    """
    # Get columns and name of table to insert
    if cols is None:
        cols = df.index.names + list(df) if index else list(df)
    colnames = [f'"{x}"' for x in cols]
    full_name = name if schema is None else f"{schema}.{name}"

    # This function is used to upload dataframe to the connection
    def upload_via_conn(con):
        with io.StringIO() as csv:
            df.to_csv(csv, sep="\t", columns=cols, index=index, header=False)
            csv.seek(0)
            with con.cursor() as cur:
                # handle replacement strategy
                if if_exists == "replace":
                    cur.execute("DELETE FROM {};".format(full_name))
                cur.copy_from(csv, full_name, columns=colnames, null="")
                cur.connection.commit()

    # If we want to replace the table, replace it
    if if_exists == "replace":
        conn.execute("DROP TABLE IF EXISTS {};".format(full_name))

    # Set up temp table creation and create
    create_want = "CREATE TEMP TABLE" if temp else "CREATE TABLE IF NOT EXISTS"
    create_query = (
        pd.io.sql.get_schema(df, name, con=conn)
        .replace("CREATE TABLE", create_want)
        .replace(f'"{name}"', full_name)
    )
    conn.execute(create_query)  # make sure the table exists

    # Set up connection based on what is passed in
    if isinstance(conn, sa.engine.base.Engine):
        with conn.connect() as con:
            upload_via_conn(con.connection)
    elif isinstance(conn, sa.engine.base.Connection):
        upload_via_conn(conn.connection)
    else:
        raise ValueError("Don't know how to handle conn with type:", type(conn))


class TempTable:
    def __init__(self, df, table_name, conn, destroy=False, **kw):
        "all kwargs passed to fast_to_sql"
        self.df = df
        self.table_name = table_name
        self.conn = conn
        self.kw = kw
        self.destroy = destroy

    def _try_empty_table(self):
        try:
            self.conn.execute("TRUNCATE TABLE {};".format(self.table_name))
        except:
            pass

    def __enter__(self):
        self._try_empty_table()
        fast_to_sql(self.df, self.conn, self.table_name, **self.kw)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._try_empty_table()
        if self.destroy:
            self.conn.execute("DROP TABLE IF EXISTS {};".format(self.table_name))


_dtype_map = {
    np.dtype("float64"): "numeric(12, 6)",
    np.dtype("float32"): "numeric(10, 4)",
    np.dtype("int64"): "INT",
    np.dtype("int32"): "SMALLINT",
    np.dtype("<M8[ns]"): "TIMESTAMP WITHOUT TIME ZONE",
    pd.core.dtypes.dtypes.DatetimeTZDtype(unit="ns", tz="UTC"): "TIMESTAMPTZ",
    np.dtype("O"): "TEXT",
}


def draft_sql_ddl_statement(df, table_name=None):
    if table_name is None:
        table_name = "REPLACE_NAME"

    cols = []
    col_comments = []

    dtypes = dict(df.dtypes)
    for c in list(df):
        dt = dtypes[c]
        if dt not in _dtype_map:
            msg = f" Don't know how to handle dtype {dt} for column {c}. Add it!"
            raise ValueError(msg)
        cols.append(f'"{c}" {_dtype_map[dt]}')
        col_comments.append(f"COMMENT ON COLUMN {table_name}.{c} is E'';")

    col_str = ",\n        ".join(cols)
    comment_str = "\n    ".join(col_comments)

    out = f"""
    CREATE TABLE {table_name} (
        {col_str}
    );

    COMMENT ON TABLE {table_name} is E'';

    {comment_str}
    """
    return textwrap.dedent(out)
