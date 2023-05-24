import datetime
import os
from pymongo import MongoClient, collection
import FileLib as fl
import mysql.connector
from mysql.connector import Error
import pandas as pd



inputpath = os.path.dirname(__file__)+'/input/'
outputpath = os.path.dirname(__file__)+'/output/'



#Mogo stuff
host = "213.184.119.120"
mongoPort = 27017
mongoUser = "Wouter"
mongoPass = "String1!"

def DBTGetMongoConnection(database, collection):
    client = MongoClient('mongodb://'+mongoUser+":"+mongoPass+"@"+host+":"+str(mongoPort))
    
    db = client[database]
    if collection not in db.list_collection_names():
        return LookupError
    collection = db[collection]
    return collection

def DBTGetAllItemsInMongoCollection(collection: collection.Collection):
    return pd.DataFrame(collection.find({}))



#MySQL Stuff

MySQLUser = mongoUser
MySQLPass = mongoPass
MySQLPort = 3306

def DBTGetMySQLConnection(database):
    try:
        connection = mysql.connector.connect(
            host=host,
            port=MySQLPort,
            database = database,
            user=MySQLUser,
            password=MySQLPass
        )

        if connection.is_connected():
            return connection

    except Error as e:
        print("Error while connecting to MySQL", e)

def DBTGetAllItemsInMySQLTable(cursor, table):
    query = f"SELECT * FROM {table}"
    cursor.execute(query)
    entries = cursor.fetchall()
    columnNames = [desc[0] for desc in cursor.description]
    return pd.DataFrame(entries,columns=columnNames)

def DBTReEncodeAddresses(dataframe: pd.DataFrame):
    dataframe["straat"] = dataframe["straat"].astype(str).apply(lambda x: x.encode('latin-1',"ignore").decode('utf8'))
    return dataframe
    

def DBTConcatMySQLAddresses(dataframe: pd.DataFrame):
    dataframe["adres"] = (
        dataframe["straat"].astype(str) 
        +" "
        +dataframe["huisnummer"].astype(str)
        +dataframe["huisletter"].apply(lambda x: x if x is not None else "").apply(str.upper)
        +dataframe["huisnummertoevoeging"].apply(lambda x: " "+ x if x is not None else "")
    )
    return dataframe

def DBTFindMatchingDFids(df1:pd.DataFrame, df2:pd.DataFrame):
    matching = df1.loc[df1['adres'].isin(df2['address']), 'id']
    return matching

def DBTDropAllOtherColumns(columnsToKeep, dataframe: pd.DataFrame):
    colsToDrop = [col for col in dataframe.columns if col not in columnsToKeep]
    dataframe.drop(colsToDrop, axis=1, inplace=True)
    return dataframe

def WriteDFToMySQL(dataframe, connection, table_name):
    cursor = connection.cursor()
    columns = list(dataframe.columns)
    # Iterate over each row in the DataFrame
    for _, row in dataframe.iterrows():
        record_id = row['id']

        # Check if the ID already exists in the database
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id = %s", (record_id,))
        result = cursor.fetchone()

        if result[0] > 0:
            # ID exists, update the record
            update_query = f"UPDATE {table_name} SET "
            update_query += ', '.join([f"{column} = %s" for column in columns])
            update_query += " WHERE id = %s"
            cursor.execute(update_query, tuple(row[column] for column in columns) + (record_id,))
        else:
            # ID does not exist, insert a new record
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
            insert_query += '(' + ', '.join(['%s' for _ in columns]) + ')'
            cursor.execute(insert_query, tuple(row[column] for column in columns))

    # Commit the changes to the database
    connection.commit()

def writeDFToMySQLv2(dataframe, connection, table_name):
    # Create a cursor object
    cursor = connection.cursor()

    # Create the INSERT INTO statement
    columns = ','.join(dataframe.columns)
    values = ','.join(['%s'] * len(dataframe.columns))
    insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

    # Convert DataFrame to a list of tuples
    data = [tuple(row) for row in dataframe.values]

    try:
        # Execute the INSERT INTO statement
        cursor.executemany(insert_statement, data)

        # Commit the changes to the database
        connection.commit()
        print("Data inserted successfully into MySQL table.")

    except mysql.connector.Error as error:
        # Rollback the transaction in case of any error
        connection.rollback()
        print(f"Error inserting data into MySQL table: {error}")

    # Close the cursor and connection
    cursor.close()




def TransferData():
    mongoCollection = DBTGetMongoConnection("kadasterdata_nl","kadasterdata")
    mongoData = DBTGetAllItemsInMongoCollection(mongoCollection)
    
    mySQLConnection = DBTGetMySQLConnection("dbDEDSv2")
    mySQLLocatieData = DBTGetAllItemsInMySQLTable(mySQLConnection.cursor(),"Locatie")
    mySQLUsableLocatieData = DBTConcatMySQLAddresses(DBTReEncodeAddresses(mySQLLocatieData))
    
    #Remove this one stupid entry
    mongoData.drop(mongoData.index[mongoData['address'] == "Van Beverningkstraat 103"], inplace = True)
    
    # Tests
    print(len(mongoData)) #Total data size
    print(len(mongoData.loc[~mongoData['address'].isin(mySQLUsableLocatieData['adres']),'address'])) #Missing records
    print(len(mongoData.loc[mongoData['address'].isin(mySQLUsableLocatieData['adres']),'address'])) #Found records
    
    # Merge the dataframes where they match
    mySQLUsableLocatieData = mySQLUsableLocatieData[mySQLUsableLocatieData['adres'].isin(mongoData['address'])]
    mergedLocatieDataset = mongoData.merge(mySQLUsableLocatieData, left_on="address", right_on="adres", how="inner")
    print(mergedLocatieDataset["id"].isnull().sum())
    
    # Add data to gebouw & merge with locatie dataset
    mySQLGebouwData = DBTGetAllItemsInMySQLTable(mySQLConnection.cursor(),"Gebouw")
    mySQLGebouwData = mySQLGebouwData[mySQLGebouwData['locatie_id'].isin(mergedLocatieDataset['id'])]
    mySQLGebouwDataMerged = mySQLGebouwData.merge(mergedLocatieDataset, left_on="locatie_id",right_on="id", how="left",suffixes=('_left', '_right'))
    mySQLGebouwDataMerged.drop("id_right",axis=1, inplace=True)
    mySQLGebouwDataMerged = mySQLGebouwDataMerged.rename(columns={"id_left":"id"})
    
    #Add prijsbepalingen
    print(mySQLGebouwDataMerged.keys())
    mysSqlPrijsBepaling = DBTGetAllItemsInMySQLTable(mySQLConnection.cursor(),"PrijsBepaling")
    current_date = datetime.datetime.now().strftime("%y-%m-%d")
    prijsbepalingDF = pd.DataFrame({
        "gebouw_id": mySQLGebouwDataMerged["id"],
        "price in euros": mySQLGebouwDataMerged["price in euros"],
        "datum": current_date
    })
    #Push to DB
    writeDFToMySQLv2(prijsbepalingDF, mySQLConnection, "PrijsBepaling")

    
    # Update the oppervlakte & remove unneeded data
    # print(mySQLGebouwDataMerged.get(["surface in m2","Oppervlakte"]))
    mask = mySQLGebouwDataMerged['Oppervlakte'].isnull()
    print(mask.sum()) #Total amount of missing data
    mySQLGebouwDataMerged.loc[mask, 'Oppervlakte'] = mySQLGebouwDataMerged.loc[mask, 'surface in m2']
    DBTDropAllOtherColumns(mySQLGebouwData.keys(),mySQLGebouwDataMerged)
    # print(mySQLGebouwDataMerged)
    # Push to DB
    # WriteDFToMySQL(mySQLGebouwDataMerged,mySQLConnection,"Gebouw")
    mySQLConnection.close()

    
TransferData()

#Zoek de locatie bij adres, zoek gebouw bij locatieID & map , zoek prijsBepaling bij gebouwID
#Gebouw match mongo ID aan locatie ID, match locatieID aan gebouw, vul oppervlakte in