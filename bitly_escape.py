import time
import requests
import sqlite3
import json

from config import BITLY_ACCESS_TOKEN, BITLY_GROUP_ID


# Function to fetch link history from Bitly API
def fetch_bitlinks(access_token, group_guid, size=50, search_after=None):
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    url = f"https://api-ssl.bitly.com/v4/groups/{group_guid}/bitlinks"
    params = {"size": size, "archived": "both"}
    if search_after:
        params["search_after"] = search_after

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            # Wait for 1 minute before retrying
            print(
                f"Failed to fetch bitlinks: {response.status_code}. Retrying after 1 minute..."
            )
            time.sleep(60)


# Function to initialize the SQLite database
def initialize_database():
    conn = sqlite3.connect("bitly_links.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS links (
            key TEXT PRIMARY KEY,
            created_at TEXT,
            link TEXT,
            long_url TEXT,
            title TEXT,
            visits INTEGER DEFAULT 0,
            json_data TEXT
        )
        """
    )
    conn.commit()
    return conn, cursor


# Function to insert or update link data into the database
def insert_or_update_link_data(cursor, link_data):
    key = link_data.get("id").split("/")[-1]
    created_at = link_data.get("created_at")
    link = link_data.get("link")
    long_url = link_data.get("long_url")
    title = link_data.get("title")
    json_data = json.dumps(link_data)

    # Handle custom bitlinks
    custom_bitlinks = link_data.get("custom_bitlinks", [])

    if not len(custom_bitlinks):
        cursor.execute(
            """
            INSERT OR REPLACE INTO links (key, created_at, link, long_url, title, visits, json_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (key, created_at, link, long_url, title, 0, json_data),
        )

    for custom_bitlink in custom_bitlinks:
        custom_key = custom_bitlink.split("/")[-1]
        cursor.execute(
            """
            INSERT OR REPLACE INTO links (key, created_at, link, long_url, title, visits, json_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (custom_key, created_at, custom_bitlink, long_url, title, 0, json_data),
        )


# Main function to fetch and store link data
def main():
    # Initialize the database
    conn, cursor = initialize_database()
    links_saved_count = 0

    size = 50
    search_after = None
    bitly_result = True

    while bitly_result:
        bitlinks_response = fetch_bitlinks(
            BITLY_ACCESS_TOKEN, BITLY_GROUP_ID, size, search_after
        )
        if bitlinks_response:
            bitlinks = bitlinks_response.get("links", [])
            pagination = bitlinks_response.get("pagination", {})

            for link in bitlinks:
                insert_or_update_link_data(cursor, link)
                print(f"Stored data for link {link['id']}")
                links_saved_count += 1
            conn.commit()

            print(
                f"Total links saved so far: {links_saved_count} (search_after = {search_after})"
            )

            # Check if there are more links to fetch
            search_after = pagination.get("search_after")
            if not search_after:
                bitly_result = False

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
