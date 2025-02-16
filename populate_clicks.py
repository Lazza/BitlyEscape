import requests
import sqlite3
import time
from config import BITLY_ACCESS_TOKEN


# Function to fetch click data for a Bitlink from Bitly API
def fetch_click_data(access_token, bitlink):
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    url = f"https://api-ssl.bitly.com/v4/bitlinks/{bitlink}/clicks"
    params = {"unit": "month", "units": -1}

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(
                f"Failed to fetch clicks for {bitlink}: {response.status_code}. Retrying after 1 minute..."
            )
            time.sleep(60)  # Wait for 1 minute before retrying


# Function to update click data in the database
def update_click_data(cursor, key, clicks):
    cursor.execute(
        """
        UPDATE links
        SET visits = ?
        WHERE key = ?
        """,
        (clicks, key),
    )


# Main function to fetch and update click data
def main():
    # Connect to the SQLite database
    conn = sqlite3.connect("bitly_links.db")
    cursor = conn.cursor()

    # Fetch all links from the database
    cursor.execute("SELECT key, link FROM links ORDER BY created_at DESC")
    links = cursor.fetchall()

    total_links = len(links)
    counter = 1
    for key, link in links:
        # Remove the http:// or https:// prefix
        bitlink = link.replace("https://", "").replace("http://", "")
        click_data = fetch_click_data(BITLY_ACCESS_TOKEN, bitlink)

        if click_data:
            total_clicks = sum(entry["clicks"] for entry in click_data["link_clicks"])
            update_click_data(cursor, key, total_clicks)
            conn.commit()
            print(
                f"({counter}/{total_links}) Updated click data for link {link}: {total_clicks} clicks"
            )

        counter += 1

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
