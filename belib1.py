import requests
import mysql.connector

# API URL
api_url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=belib&q=&rows=1000"

try:
    # Fetch data from the API
    response = requests.get(api_url)
    response.raise_for_status() 
    data = response.json()
    records = data['records']  # 'records' should contain the data
except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
    exit()
except KeyError as e:
    print(f"Error in response structure: {e}")
    exit()

# Database configuration (adjust to your Workbench settings)
db_config = {
    'user': 'root', 
    'password': 'H68bhxcx@',
    'host': 'localhost',  
    'database': 'belib'  
}

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Insert data into the table (assuming table 'belib_points' exists)
    for record in records:
        cursor.execute('''
            INSERT INTO belib_points (name, latitude, longitude, address, number_of_points)
            VALUES (%s, %s, %s, %s, %s)
        ''', (record['fields']['name'], record['fields']['geo_point_2d'][0], record['fields']['geo_point_2d'][1], record['fields']['address'], record['fields']['number_of_points']))

    conn.commit()  

    print("Data inserted successfully!")

except mysql.connector.Error as e:
    print(f"Database error: {e}")
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'conn' in locals() and conn:
        conn.close()


