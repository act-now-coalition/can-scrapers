import io
from contextlib import closing
from typing import List, Optional, Type, Union

import pandas as pd
from sqlalchemy.engine.base import Engine

from can_tools.models import (TemptableOfficialHasLocation,
                              TemptableOfficialNoLocation)


def fast_append_to_sql(
    df: pd.DataFrame,
    engine: Engine,
    table_type: Union[
        Type[TemptableOfficialNoLocation], Type[TemptableOfficialHasLocation]
    ],
):
    table = table_type.__table__
    cols = [x.name for x in table.columns if x.name != "id"]
    colnames = [f'"{x}"' for x in cols]
    temp_df = df.reset_index()

    # make sure we have the columns
    have_cols = set(list(temp_df))
    missing_cols = set(cols) - have_cols
    if len(missing_cols) > 0:
        msg = "Missing columns {}".format(", ".join(list(missing_cols)))
        raise ValueError(msg)

    with closing(engine.connect()) as con:

        if engine.dialect.name == "postgresql":
            dest = (
                "{}.{}".format(table.schema, table.name)
                if table.schema is not None
                else table.name
            )
            with io.StringIO() as csv:
                temp_df.to_csv(csv, sep="\t", columns=cols, index=False, header=False)
                csv.seek(0)
                with closing(con.connection.cursor()) as cur:
                    cur.copy_from(csv, dest, columns=colnames, null="")
                    cur.connection.commit()
        elif engine.dialect.name == "sqlite":
            # pandas is ok for sqlite
            temp_df[cols].to_sql(
                table.name, engine, if_exists="append", index=False, chunksize=500_000
            )
        else:
            raise NotImplementedError("Only implemented for sqlite and postgres")
