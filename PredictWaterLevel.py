# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 11:08:49 2018
This purpose of this program is to predict the reservoir volume for the next day. It does so by performing the following steps:
1. Connects to an Azure SQL Database
2. Retrives data from 3 simple tables-- Reservoir Rainfall, Reservoir Temperature and Reservoir Volume
3. The relevant row is then retrieved and formatted as a JSON object
4. The JSON object is then passed on to an Azure Machine Learning Web service which is a callable version of an autoregressive model
5. This returns the predicted volume as a json object
6. The JSON object is then parsed and the predicted volume table is updated.

Wherever the term 'xxx' is encountered it should be replace with the approprite database name, credentials and api keys

@author: Rajagopalanra
"""
import urllib.request
from collections import OrderedDict
import pyodbc
import json
import datetime
import sys


PredictedDate =str(sys.argv[1]) # Date is dynamically passed in
server = 'xxx'
database = 'xxx'
username = 'xxx'
password = 'xxx'
driver= '{ODBC Driver 11 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
cursor.execute("""
       SELECT CONVERT(VARCHAR, RV.ReadingDate, 23)as 'Date', '0' as 'Volume', 
       LAG (RV.ReadingValue,1) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-1)',
       LAG (RV.ReadingValue,2) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-2)',
       LAG (RV.ReadingValue,3) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-3)',
	   LAG (RV.ReadingValue,4) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-4)',
	   LAG (RV.ReadingValue,5) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-5)',
	   LAG (RV.ReadingValue,6) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-6)',
	   LAG (RV.ReadingValue,7) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-7)',
	   LAG (RV.ReadingValue,8) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-8)',
	   LAG (RV.ReadingValue,9) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-9)',
	   LAG (RV.ReadingValue,9) OVER (ORDER BY RV.ReadingDate) AS 'Volume(-10)',
	   RR.ReadingValue AS 'Daily Rain To 9am',
	   LAG (RR.ReadingValue,1) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-1)',
	   LAG (RR.ReadingValue,2) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-2)',
	   LAG (RR.ReadingValue,3) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-3)',
	   LAG (RR.ReadingValue,4) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-4)',
	   LAG (RR.ReadingValue,5) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-5)',
	   LAG (RR.ReadingValue,6) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-6)',
	   LAG (RR.ReadingValue,7) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-7)',
	   LAG (RR.ReadingValue,8) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-8)',
	   LAG (RR.ReadingValue,9) OVER (ORDER BY RR.ReadingDate) AS 'Daily Rain To 9am(-9)',
	   LAG (RT.ReadingValue,1) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-1)',
	   LAG (RT.ReadingValue,2) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-2)',
	   LAG (RT.ReadingValue,3) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-3)',
	   LAG (RT.ReadingValue,4) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-4)',
	   LAG (RT.ReadingValue,5) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-5)',
	   LAG (RT.ReadingValue,6) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-6)',
	   LAG (RT.ReadingValue,7) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-7)',
	   LAG (RT.ReadingValue,8) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-8)',
	   LAG (RT.ReadingValue,9) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-9)',
	   LAG (RT.ReadingValue,10) OVER (ORDER BY RT.ReadingDate) AS 'Air Temperature(-10)'
       FROM Reservoir1Volume RV
       Join Reservoir1Temperature RT ON
       RV.ReadingDate = RT.ReadingDate
       Join Reservoir1Rainfall RR ON
       RV.ReadingDate = RR.ReadingDate""")


#Poupulate a list of rows
rows = [x for x in cursor]
cols = [x[0] for x in cursor.description]
#Populate  a list of dictionaries with result of the query- Ordered dictionary is required as by default dictionaries are not ordered
ResultSetList = []
for row in rows:
  RowValueDict = OrderedDict({})
  for prop, val in zip(cols, row):
    RowValueDict[prop] = val
  ResultSetList.append(RowValueDict)
#Use list comprehension feature of python to get the list we need to predict- Note the variable date was passed in
  Rowlist= [d for d in ResultSetList if d.get('Date')==PredictedDate]

#Create a new dictionary which has this list as an input and a empty global parameter dictionary
  data = {"Inputs": {"input1":Rowlist,},
          "GlobalParameters": {}
         }
  

#Create a string representation of the json string to be passed on to the webservice
HVSJSON = json.dumps(data)

#Encode the data into byte form to pass on to the webservice
body = str.encode(HVSJSON)

url = 'https://ussouthcentral.services.azureml.net/workspaces/eaec4b35848940659b96c599ba15a0a2/services/fa6260eedcd043f588dc6f03a5cc1f25/execute?api-version=2.0&format=swagger'
api_key = 'xxx'
headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

req = urllib.request.Request(url, body, headers)

try:
    response = urllib.request.urlopen(req)

    result = response.read()
    print(result)
except urllib.error.HTTPError as error:
    print("The request failed with status code: " + str(error.code))

#Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
    print(error.info())
    print(json.loads(error.read().decode("utf8", 'ignore')))
#The resultset is a byte object which needs to be decoded to a string
decoded_result = result.decode()

#The string in turr needs to be loaded as a dictionary for further parsing
result_dict = json.loads(decoded_result)
result_list= result_dict['Results']['output1']
predicted_dict ={}
predicted_dict= result_list[0]
PredictedVolumeString=predicted_dict['PredictedVolume']
PredictedValue= float(PredictedVolumeString)
#The statement below took a lot of time to build as SQL serve expecte the date to be included as a single quote
sql="""
       INSERT INTO Reservoir1PredictedVolume 
       VALUES (Convert(date,'%s'),%s)""" %(PredictedDate,PredictedValue)
cursor.execute(sql)
cnxn.commit()
cnxn.close()