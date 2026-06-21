from pathlib import Path
from src.utils.postgres_utils import (get_postgres_connection, run_sql_file)


SQL_DIR = Path("src/sql/")


def main():

    conn = get_postgres_connection()

    cursor = conn.cursor()

    sql_files = [
        "postgres_ddl.sql"
    ]

    for sql_file in sql_files:

        run_sql_file(
            cursor,
            SQL_DIR / sql_file
        )

    conn.commit()

    cursor.close()
    conn.close()

    print("Postgres setup completed")


if __name__ == "__main__":
    main()