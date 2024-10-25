import csv
from neo4j import GraphDatabase

# Replace with your Neo4j Aura credentials
uri = "neo4j+s://1c8010b1.databases.neo4j.io"
user = "neo4j"  # Default username for Aura
password = "VS4XqVmt4_YN6m6_7y5VoVIQCgYghry-sDgXIeiJ7ws"  # The password you set during setup

# Initialize the driver
driver = GraphDatabase.driver(uri, auth=(user, password))

# Function to create nodes and relationships
def create_nodes_and_relationships(tx, client1, client2, start_time, end_time):
    query = """
    MERGE (c1:Client {id: $client1})
    MERGE (c2:Client {id: $client2})
    CREATE (c1)-[r:CALL {
        start_time: $start_time, 
        end_time: $end_time
    }]->(c2)
    """
    tx.run(query, client1=client1, client2=client2, start_time=start_time, end_time=end_time)

# Function to load CSV data and send it to Neo4j
def load_csv_to_neo4j(csv_file_path):
    with driver.session() as session:
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                session.write_transaction(
                    create_nodes_and_relationships,
                    row['Client1'],
                    row['Client2'],
                    row['Start_Time'],
                    row['End_Time']
                )

# Path to your CSV file
csv_file_path = "../../Data/adjusted_phone_calls.csv"

# Load data from CSV and send to Neo4j
load_csv_to_neo4j(csv_file_path)

# Close the Neo4j connection
driver.close()