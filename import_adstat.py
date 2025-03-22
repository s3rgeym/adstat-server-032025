#!/usr/bin/env python
import datetime
import os
import sys
from functools import partial
from typing import Any, Dict, List

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv()

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"

CSI = "\033["
COLORS = {
    "red": 91,
    "green": 92,
    "yellow": 93,
    "blue": 94,
    "magenta": 95,
    "cyan": 96,
    "white": 97,
    "reset": 0,
}


def colored(color: str, text: str, **kwargs) -> None:
    color_code = COLORS.get(color, COLORS["reset"])
    print_err(f"{CSI}{color_code}m{text}{CSI}{COLORS['reset']}m", **kwargs)


print_err = partial(print, file=sys.stderr)


def create_database(db_config: Dict[str, Any]) -> None:
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
            sslmode=db_config["sslmode"],
        )
        connection.autocommit = True
        cursor = connection.cursor()

        db_name = db_config["dbname"]
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = {}").format(
                sql.Literal(db_name)
            )
        )
        if not cursor.fetchone():
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
            )
            colored("green", f"Database {db_name} created.")
        else:
            colored("yellow", f"Database {db_name} already exists.")

        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        colored("red", f"Error creating database: {e}")


def create_table(cursor: psycopg2.extensions.cursor, table_name: str) -> None:
    try:
        cursor.execute(
            sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                date DATE,
                spent REAL,
                impressions REAL,
                goals REAL,
                price_target REAL,
                cpm REAL,
                object VARCHAR(255),
                account_uid VARCHAR(255),
                account_name VARCHAR(255),
                ad_type VARCHAR(31),
                clicks INT,
                cpc REAL,
                ctr REAL,
                UNIQUE (date, object, account_uid)
            );
        """).format(sql.Identifier(table_name))
        )
        cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS idx_date ON {} (date);").format(
                sql.Identifier(table_name)
            )
        )
        cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS idx_object ON {} (object);").format(
                sql.Identifier(table_name)
            )
        )
        cursor.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS idx_account_uid ON {} (account_uid);"
            ).format(sql.Identifier(table_name))
        )
        colored("green", f"Table {table_name} created.")
    except psycopg2.Error as e:
        colored("red", f"Error creating table: {e}")
        sys.exit(1)


def save_data(
    cursor: psycopg2.extensions.cursor,
    table_name: str,
    data: List[Dict[str, Any]],
    batch_size: int = 1000,
) -> None:
    try:
        for i in range(0, len(data), batch_size):
            batch = data[i: i + batch_size]
            cursor.executemany(
                sql.SQL("""
                    INSERT INTO {} (
                        date, spent, impressions, goals, price_target, cpm, object, account_uid, account_name, ad_type, clicks, cpc, ctr
                    ) VALUES (
                        %(date)s, %(spent)s, %(impressions)s, %(goals)s, %(price_target)s, %(cpm)s, %(object)s, %(account_uid)s, %(account_name)s, %(ad_type)s, %(clicks)s, %(cpc)s, %(ctr)s
                    )
                    ON CONFLICT (date, object, account_uid) DO UPDATE SET
                        spent = EXCLUDED.spent,
                        impressions = EXCLUDED.impressions,
                        goals = EXCLUDED.goals,
                        price_target = EXCLUDED.price_target,
                        cpm = EXCLUDED.cpm,
                        account_name = EXCLUDED.account_name,
                        ad_type = EXCLUDED.ad_type,
                        clicks = EXCLUDED.clicks,
                        cpc = EXCLUDED.cpc,
                        ctr = EXCLUDED.ctr;
                """).format(sql.Identifier(table_name)),
                batch
            )
            colored(
                "green",
                f"Inserted/updated {len(batch)} records from index {i} to {i + len(batch)}.",
            )
    except psycopg2.Error as e:
        colored("red", f"Error inserting or updating data: {e}")


def fetch_data(username: str, password: str) -> List[Dict[str, Any]]:
    try:
        session = requests.session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"})
        payload = {
            "username": username,
            "password": password,
        }
        r = session.post("https://client.adstat.pro/api/v2/login", payload)
        login_result = r.json()
        #print_err(login_result)
        access_token = login_result["access_token"]

        now = datetime.datetime.now(datetime.UTC)

        date_from = (now - datetime.timedelta(hours=1, minutes=5)).strftime(
            DATETIME_FORMAT
        )
        date_to = now.strftime(DATETIME_FORMAT)

        payload = {
            "date": {"date_from": date_from, "date_to": date_to},
            "platform": [10],
            "partner": [2],
            "campaign": [],
            "group_time": 1,
            "groupings": [
                {"name": "object"},
                #{"name": "campaign"},
                {"name": "account", "type": 1},
                {"name": "date", "type": 1},
            ],
            "object": [],
            "sub_client": [],
            "type_cab": None,
            "account_uids": [],
            "currency_code": "EUR",
            "use_account_currency": False,
        }
        r = session.post(
            "https://client.adstat.pro/api/report/tgview", 
            json=payload,
            headers={"Authorization": f"Bearer {access_token}"})
        return r.json().get("results", [])
    except requests.RequestException as e:
        colored("red", f"Error fetching data from server: {e}")
        sys.exit(1)


def main() -> None:
    db_config = {
        "dbname": os.getenv("DB_NAME", "adstat_db"),
        "user": os.getenv("DB_USER", "docker"),
        "password": os.getenv("DB_PASSWORD", "secret"),
        "host": os.getenv("DB_HOST", "postgres"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "sslmode": os.getenv("DB_SSLMODE", "require"),
    }

    try:
        create_database(db_config)

        connection = psycopg2.connect(**db_config)
        connection.autocommit = True
        cursor = connection.cursor()
        table_name = "statistics"
        create_table(cursor, table_name)

        data = fetch_data(
            os.getenv("ADSTAT_USERNAME"),
            os.getenv("ADSTAT_PASSWORD"),
        )

        save_data(cursor, table_name, data)

        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        colored("red", f"Database error: {e}")


if __name__ == "__main__":
    main()

