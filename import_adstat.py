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
                id SERIAL PRIMARY KEY,
                date DATE,
                unit BIGINT,
                campaign_plat VARCHAR(1023),
                ad_text TEXT,
                target_topics TEXT[],
                target_channels TEXT[],
                target_langs TEXT[],
                spent REAL,
                impressions REAL,
                goals REAL,
                price_target REAL,
                cpm REAL,
                object VARCHAR(255),
                account_uid VARCHAR(255),
                account_name VARCHAR(255),
                target_countries TEXT[],
                target_user_locations TEXT[],
                target_user_channels TEXT[],
                ad_type VARCHAR(31),
                ad_id INT,
                clicks INT,
                cpc REAL,
                ctr REAL,
                promote_url VARCHAR(2000),
                website_name VARCHAR(1023),
                views_per_users INT,
                button_type VARCHAR(255),
                media_type VARCHAR(255),
                only_crypto BOOLEAN,
                exclude_crypto BOOLEAN,
                UNIQUE (date, unit, ad_id)
            );
        """).format(sql.Identifier(table_name))
        )

        cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS idx_unit ON {} (unit);").format(
                sql.Identifier(table_name)
            )
        )
        cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS idx_date ON {} (date);").format(
                sql.Identifier(table_name)
            )
        )
        cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS idx_ad_id ON {} (ad_id);").format(
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
                    date, unit, campaign_plat, ad_text, target_topics, target_channels,
                    target_langs, spent, impressions, goals, price_target, cpm, object,
                    account_uid, account_name, target_countries, target_user_locations,
                    target_user_channels, ad_type, ad_id, clicks, cpc, ctr, promote_url,
                    website_name, views_per_users, button_type, media_type, only_crypto, exclude_crypto
                ) VALUES (
                    %(date)s, %(unit)s, %(campaign_plat)s, %(ad_text)s, %(target_topics)s, %(target_channels)s,
                    %(target_langs)s, %(spent)s, %(impressions)s, %(goals)s, %(price_target)s, %(cpm)s, %(object)s,
                    %(account_uid)s, %(account_name)s, %(target_countries)s, %(target_user_locations)s,
                    %(target_user_channels)s, %(ad_type)s, %(ad_id)s, %(clicks)s, %(cpc)s, %(ctr)s, %(promote_url)s,
                    %(website_name)s, %(views_per_users)s, %(button_type)s, %(media_type)s, %(only_crypto)s, %(exclude_crypto)s
                )
                ON CONFLICT (date, unit, ad_id) DO UPDATE SET
                    campaign_plat = EXCLUDED.campaign_plat,
                    ad_text = EXCLUDED.ad_text,
                    target_topics = EXCLUDED.target_topics,
                    target_channels = EXCLUDED.target_channels,
                    target_langs = EXCLUDED.target_langs,
                    spent = EXCLUDED.spent,
                    impressions = EXCLUDED.impressions,
                    goals = EXCLUDED.goals,
                    price_target = EXCLUDED.price_target,
                    cpm = EXCLUDED.cpm,
                    object = EXCLUDED.object,
                    account_uid = EXCLUDED.account_uid,
                    account_name = EXCLUDED.account_name,
                    target_countries = EXCLUDED.target_countries,
                    target_user_locations = EXCLUDED.target_user_locations,
                    target_user_channels = EXCLUDED.target_user_channels,
                    ad_type = EXCLUDED.ad_type,
                    clicks = EXCLUDED.clicks,
                    cpc = EXCLUDED.cpc,
                    ctr = EXCLUDED.ctr,
                    promote_url = EXCLUDED.promote_url,
                    website_name = EXCLUDED.website_name,
                    views_per_users = EXCLUDED.views_per_users,
                    button_type = EXCLUDED.button_type,
                    media_type = EXCLUDED.media_type,
                    only_crypto = EXCLUDED.only_crypto,
                    exclude_crypto = EXCLUDED.exclude_crypto;
            """).format(sql.Identifier(table_name)),
                batch,
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
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        payload = {
            "username": username,
            "password": password,
        }
        r = session.post("https://client.adstat.pro/api/v2/login", payload)
        login_result = r.json()
        print_err(login_result)
        access_token = login_result["access_token"]
        session.headers.update({"Authorization": f"Bearer {access_token}"})

        now = datetime.datetime.now(datetime.UTC)

        date_from = (now - datetime.timedelta(hours=1, minutes=15)).strftime(
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
                {"name": "campaign"},
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
            "https://client.adstat.pro/api/report/tgview", json=payload)
        return r.json().get("results", [])
    except requests.RequestException as e:
        colored("red", f"Error fetching data from server: {e}")
        sys.exit(1)


def main() -> None:
    db_config = {
        "dbname": os.getenv("DB_NAME", "adstat_db"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "sslmode": os.getenv("DB_SSLMODE", "require"),
    }

    table_name = "ad_campaigns"

    try:
        create_database(db_config)

        connection = psycopg2.connect(**db_config)
        connection.autocommit = True
        cursor = connection.cursor()
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

