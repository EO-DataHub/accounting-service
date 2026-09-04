"""Guards on the schema the models declare.

No database. These read the metadata directly, which is what `create_all` emits and what
Alembic compares a revision against, so a fault here is caught before it reaches either.
"""

from datetime import datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func
from sqlalchemy.dialects import postgresql
from sqlmodel import Field, SQLModel

from accounting_service import models  # noqa: F401  (importing registers the tables)
from accounting_service.models import aware_timestamp

PG = postgresql.dialect()


def declared_timestamp_columns() -> list[tuple[str, str, str]]:
    """Every timestamp column in the schema, as (table, column, compiled PostgreSQL type)."""
    return [
        (table_name, column.name, column.type.compile(PG))
        for table_name, table in sorted(SQLModel.metadata.tables.items())
        for column in table.columns
        if "TIMESTAMP" in column.type.compile(PG)
    ]


def test_every_timestamp_column_is_timezone_aware() -> None:
    """No column may be TIMESTAMP WITHOUT TIME ZONE.

    This is the guard for `aware_timestamp()`. SQLModel maps a bare `datetime` to a naive
    column, which discards the offset on write and hands back a naive value on read, and
    nothing else in the suite would notice: the value is only wrong by the connection's
    offset, so on a UTC server the tests would pass and a differently configured server
    would be silently an hour out.

    Three separate instances of that failure have already been fixed in this service. This
    test needs no maintenance as the schema grows, because it finds the columns itself.
    """
    columns = declared_timestamp_columns()

    assert columns, "no timestamp columns found at all, which means this guard is not looking anywhere"

    naive = [f"{table}.{column}" for table, column, rendered in columns if "WITH TIME ZONE" not in rendered]

    assert not naive, (
        f"these columns are TIMESTAMP WITHOUT TIME ZONE: {naive}. Declare them with "
        f"aware_timestamp() from accounting_service.models rather than a bare datetime."
    )


class TestAwareTimestamp:
    """The factory behaves in every shape the models need.

    Declared on throwaway tables which are removed again, so the real schema is untouched.
    An annotated type was tried first and silently lost its `sa_type` when combined with
    `| None` or with an explicit `Field(...)`; these cases are what caught that.
    """

    @staticmethod
    def _column_type(**field_kwargs: object) -> str:
        name = f"_probe_{uuid4().hex}"

        model = type(
            f"Probe_{name}",
            (SQLModel,),
            {
                "__tablename__": name,
                "__annotations__": {"uuid": UUID, "at": datetime | None},
                "uuid": Field(default_factory=uuid4, primary_key=True),
                "at": aware_timestamp(**field_kwargs),  # type: ignore[arg-type]
            },
            table=True,
        )
        assert model is not None

        table = SQLModel.metadata.tables[name]
        try:
            return table.c.at.type.compile(PG)
        finally:
            SQLModel.metadata.remove(table)

    def test_plain(self) -> None:
        assert "WITH TIME ZONE" in self._column_type()

    def test_nullable(self) -> None:
        assert "WITH TIME ZONE" in self._column_type(default=None)

    def test_with_a_sql_function_default(self) -> None:
        assert "WITH TIME ZONE" in self._column_type(default=func.now())

    def test_indexed(self) -> None:
        assert "WITH TIME ZONE" in self._column_type(index=True)

    def test_a_bare_datetime_is_naive_which_is_why_the_factory_exists(self) -> None:
        """Pins the SQLModel behaviour the factory works around.

        If a future SQLModel release defaults to timezone-aware columns, this fails and the
        factory can be reconsidered.
        """
        name = f"_probe_{uuid4().hex}"
        model = type(
            f"Bare_{name}",
            (SQLModel,),
            {
                "__tablename__": name,
                "__annotations__": {"uuid": UUID, "at": datetime},
                "uuid": Field(default_factory=uuid4, primary_key=True),
            },
            table=True,
        )
        assert model is not None

        table = SQLModel.metadata.tables[name]
        try:
            rendered = table.c.at.type.compile(PG)
        finally:
            SQLModel.metadata.remove(table)

        assert rendered == "TIMESTAMP WITHOUT TIME ZONE", rendered


def test_check_constraints_are_named() -> None:
    """An anonymous check constraint cannot be tracked by Alembic.

    Autogenerate matches check constraints by name and can compare nothing else, because
    PostgreSQL rewrites the expression it stores. An unnamed one is reported as removed on
    every run, and no later revision can reference it. Naming them cost a rename migration
    against the deployed databases once already.
    """
    unnamed = [
        f"{table_name}.{constraint}"
        for table_name, table in sorted(SQLModel.metadata.tables.items())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "CheckConstraint" and not constraint.name
    ]

    assert not unnamed, f"unnamed check constraints: {unnamed}"


@pytest.mark.parametrize("expected", ["billingevent_day_aggregate_index", "billingevent_month_aggregate_index"])
def test_the_hand_managed_expression_indexes_are_declared(expected: str) -> None:
    """These two are excluded from autogenerate, so nothing else notices if they go.

    `include_object` in alembic/env.py hides them from comparison *and* from rendering, so
    `alembic check` reports clean when they are missing. See the comment on
    UNCOMPARED_INDEXES there.
    """
    declared = {str(index.name) for index in SQLModel.metadata.tables["billing_event"].indexes if index.name}

    assert expected in declared, f"{expected} is not declared; declared indexes are {sorted(declared)}"
