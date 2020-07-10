#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 10:06:13 2020

@author: goncalopinto
"""
import re
import pandas as pd
from pandas.io.json import json_normalize
import requests
import pymysql
from tables_update import*
from query_analysis_functions import*
from tables_clean_pop import*


#check if table already exists

connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
    
#Create cursor
cursor = connection.cursor()
try:
    #create the query
    query_rating_17 = (
       "SELECT* FROM main_data;"
        )
    a=1
    #execute query
    cursor.execute(query_rating_17)
except:
    a=0

#Close connection
connection.close()



print("Welcome IronHack")
print("")
print("Currently you have available 2 options (for other options please contact our sales team):")
print("""
      (1)WebScrapping
      (2)See analysis
      
      """)
print("")


    
option = input("Please select what you would like to do using the respective numbers ")


while option not in ["1","2"]:
    option = input("Please select a valid option (1,2) ")

if option == "1":
    if a == 0:
        fresh()
    else:
        updater()
elif option == "2":
    print("Currently you have available 3 options (for other options please contact our sales team):")
    print("""
      (1)Com Quant an
      (2)sec analysis
      (3)third an
      (4)final market analysis
      
      """)
     print("")


    
    option_an = input("Please select what you would like to do using the respective numbers ")
    
    while option_an not in ["1","2","3","4"]:
        option_an = input("Please select a valid option (1,2,3,4) ")
    if option_an == "1":
        first_analysis()
    elif option_an == "2":
        secound_analysis()
    elif option_an == "3":
        third_analysis()
    else:
        final_market_analysis()
        
    
    
    
