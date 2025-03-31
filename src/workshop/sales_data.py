import json
import logging
import os
from typing import Optional, List
from terminal_colors import TerminalColors as tc

import asyncpg
import pandas as pd

POSTGRES_CONNECTION_STRING = os.getenv("POSTGRES_CONNECTION_STRING")
DB_SCHEMA = "contoso"

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class SalesData:
    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Establish a connection pool to the database."""
        db_uri = POSTGRES_CONNECTION_STRING

        try:
            self.pool = await asyncpg.create_pool(dsn=db_uri)
            logger.info("Database connection pool created.")
        except Exception as e:
            logger.exception("Failed to connect to the database", exc_info=e)

    async def close(self) -> None:
        """Close the database connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed.")

    async def fetch_list(self, query: str, column: str) -> List[str]:
        """Helper function to fetch a list of values from a query."""
        if not self.pool:
            return []

        async with self.pool.acquire() as conn:
            try:
                return [row[column] for row in await conn.fetch(query)]
            except Exception as e:
                logger.error(f"Query failed: {query}, Error: {e}")
                return []

    async def get_database_info(self) -> str:
        """Retrieve database schema and common query fields."""
        if not self.pool:
            return "Database connection is not established."

        schema_query = f"""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1
            ORDER BY table_name, ordinal_position;
        """

        async with self.pool.acquire() as conn:
            try:
                schema_data = await conn.fetch(schema_query, DB_SCHEMA)
            except Exception as e:
                logger.error(f"Schema query failed: {e}")
                return "Error fetching schema information."

        tables = {}
        for row in schema_data:
            tables.setdefault(row["table_name"], []).append(f"{row['column_name']}: {row['data_type']}")

        database_info = "\n".join(
            [f"Table {DB_SCHEMA}.{table}: Columns: {', '.join(cols)}" for table, cols in tables.items()]
        )

        # Fetch unique values for important fields
        queries = {
            "Regions": ("SELECT DISTINCT region FROM contoso.sales_data;", "region"),
            "Product Types": ("SELECT DISTINCT product_type FROM contoso.sales_data;", "product_type"),
            "Product Categories": ("SELECT DISTINCT main_category FROM contoso.sales_data;", "main_category"),
            "Reporting Years": ("SELECT DISTINCT year FROM contoso.sales_data ORDER BY year;", "year"),
        }

        results = {key: await self.fetch_list(query, column) for key, (query, column) in queries.items()}

        # Append additional metadata
        for key, values in results.items():
            database_info += f"\n{key}: {', '.join(map(str, values))}"

        return database_info

    async def async_fetch_sales_data_using_sqlite_query(self, postgres_query: str) -> str:
        """Execute a PostgreSQL query and return the result as a JSON string."""
        if not self.pool:
            return json.dumps({"error": "Database connection is not established."})

        print((f"{tc.BLUE}Executing query: {postgres_query}{tc.RESET}\n"))

        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(postgres_query)
                if not rows:
                    return json.dumps("The query returned no results. Try a different question.")

                return pd.DataFrame(rows, columns=rows[0].keys()).to_json(index=False, orient="split")

            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                return json.dumps({"error": str(e), "query": postgres_query})