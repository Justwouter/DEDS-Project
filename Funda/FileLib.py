from csv import DictWriter, writer
import os
import json
import pyodbc
import sqlite3



def WriteDataToJSON(outfile, data):
    EnsureFileExists(outfile)
    with open(outfile, 'w', encoding="utf8", newline="") as out:
        json.dump(data, out, indent=2, ensure_ascii=False) 
        
def WriteDataToCSV(outfile, data):
    EnsureFileExists(outfile)
    with open(outfile, 'w', encoding="utf8", newline="") as out:
        write = writer(out)
        write.writerows(data)
        
def EnsureFileExists(outfile):
    directory = os.path.dirname(outfile)
    if not os.path.exists(directory):
        os.makedirs(directory)
    if not os.path.exists(outfile):
        open(outfile, 'w').close()
    
def saveDictToJSON(output, data):
    with open(output, 'w', encoding="utf8", newline="") as out:
        json.dump(data, out, ensure_ascii=False)  
    
def saveDictToCSV(output, data):
    with open(output, 'w', encoding="utf8", newline="") as out:
        write = DictWriter(out, fieldnames=data[0].keys())
        write.writeheader()
        for entry in data:
            write.writerow(entry)
    

def saveDictToSQLITE(database, table, data):
    # connect to the database
    conn = sqlite3.connect(database)
    c = conn.cursor()

    # create the table if it doesn't exist
    column_names = ", ".join(data.keys())
    c.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, {column_names})")

    # Check if data already exists
    placeholders = " AND ".join([f"{key} = ?" for key in data.keys()])
    values = tuple(data.values())
    c.execute(f"SELECT * FROM {table} WHERE {placeholders}", values)
    existing_data = c.fetchone()

    # insert the data into the table if it doesn't already exist
    if not existing_data:
        placeholders = ", ".join("?" * len(data))
        c.execute(f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})", values)


    # commit the changes and close the connection
    conn.commit()
    conn.close()