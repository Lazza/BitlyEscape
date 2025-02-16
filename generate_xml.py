import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime


# Function to generate XML from SQLite database
def generate_xml(database_path, output_xml_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Fetch all links from the database
    cursor.execute("SELECT key, link, long_url, title, visits, created_at FROM links")
    links = cursor.fetchall()

    # Create the root element
    redirection = ET.Element("redirection")
    module = ET.SubElement(redirection, "module", name="YOURLS", id="1", type="wp")
    group = ET.SubElement(
        module,
        "group",
        id="1",
        name="YOURLS",
        status="enabled",
        position="0",
        tracking="1",
    )

    # Initialize position counter
    position_counter = 0

    # Add item elements for each link
    for key, link, long_url, title, visits, created_at in links:
        # Format the created_at datetime string
        created_at_formatted = datetime.strptime(
            created_at, "%Y-%m-%dT%H:%M:%S%z"
        ).strftime("%Y-%m-%d %H:%M:%S")

        item = ET.SubElement(
            group, "item", id=key, position=str(position_counter), status="enabled"
        )
        ET.SubElement(item, "source").text = f"/{key}/"
        ET.SubElement(item, "title").text = title
        ET.SubElement(item, "ip").text = ""
        ET.SubElement(item, "match", type="url", regex="0")
        ET.SubElement(item, "action", type="url", code="301").text = long_url
        ET.SubElement(item, "statistic", count=str(visits), access=created_at_formatted)

        # Increment the position counter
        position_counter += 1

    # Write the XML to a file
    tree = ET.ElementTree(redirection)
    tree.write(output_xml_path, encoding="utf-8", xml_declaration=True)

    print(f"Generated XML file with {len(links)} links!")

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    generate_xml("bitly_links.db", "bitly_links.xml")
