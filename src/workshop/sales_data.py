import json
import logging
import os
from typing import Optional, List, Dict
import asyncpg
import pandas as pd
from terminal_colors import TerminalColors as tc

POSTGRES_CONNECTION_STRING = os.getenv("POSTGRES_CONNECTION_STRING")
DB_SCHEMA = "contoso"

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class SalesData:
    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Establish a connection pool to the database."""
        try:
            self.pool = await asyncpg.create_pool(dsn=POSTGRES_CONNECTION_STRING)
            logger.info("Database connection pool created.")
        except Exception as e:
            logger.exception("Failed to connect to the database", exc_info=e)
            raise

    async def close(self) -> None:
        """Close the database connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed.")

    async def fetch_list(self, query: str, column: str) -> List[str]:
        """Fetch a list of values from the database."""
        if not self.pool:
            logger.warning("Database connection is not established.")
            return []

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [row[column] for row in rows]
        except Exception as e:
            logger.error(f"Failed to execute query: {query} | Error: {e}")
            return []

    async def get_database_info(self) -> str:
        """Retrieve database schema and common query fields."""
        if not self.pool:
            return "Database connection is not established."

        schema_query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1
            ORDER BY table_name, ordinal_position;
        """

        try:
            async with self.pool.acquire() as conn:
                schema_data = await conn.fetch(schema_query, DB_SCHEMA)
        except Exception as e:
            logger.error(f"Schema query failed: {e}")
            return "Error fetching schema information."

        tables: Dict[str, List[str]] = {}
        for row in schema_data:
            tables.setdefault(row["table_name"], []).append(f"{row['column_name']}: {row['data_type']}")

        database_info = [
            f"Table {DB_SCHEMA}.{table}: Columns: {', '.join(cols)}"
            for table, cols in tables.items()
        ]

        # Important fields and their queries
        field_queries = {
            "Regions": "SELECT DISTINCT region FROM contoso.sales_data;",
            "Product Types": "SELECT DISTINCT product_type FROM contoso.sales_data;",
            "Product Categories": "SELECT DISTINCT main_category FROM contoso.sales_data;",
            "Reporting Years": "SELECT DISTINCT year FROM contoso.sales_data ORDER BY year;",
        }

        for field, query in field_queries.items():
            values = await self.fetch_list(query, query.split()[2])
            database_info.append(f"{field}: {', '.join(map(str, values))}")

        return "\n".join(database_info)

    async def async_fetch_sales_data(self, query: str) -> str:
        """Execute a query and return the result as a JSON string."""
        if not self.pool:
            return json.dumps({"error": "Database connection is not established."})

        print((f"\n{tc.BLUE}Executing query: {query}{tc.RESET}\n"))

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                if not rows:
                    return json.dumps("The query returned no results. Try a different question.")

                df = pd.DataFrame(rows, columns=rows[0].keys())
                return df.to_json(index=False, orient="split")
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return json.dumps({"error": str(e), "query": query})