#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 09:15:30 2020

"""

import pymysql

def first_analysis():
    #esses aqui são analises dos ratings e market shares de todos os anos e todas as escolas separadamente
    
    #2019
    def q_rating_2019():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_rating_19 = (
            "Select count(id),"
            "avg(curriculum),"
            "avg(job_support),"
            "school,"
            "count(id)/(select count(id) from main_data where  query_date BETWEEN '2019-01-01' AND '2020-01-01')*100 as share_percentage"
            "from main_data"
            "where  query_date BETWEEN '2019-01-01' AND '2020-01-01'"
            "group by school;"
            )
        
        #execute query
        cursor.execute(query_rating_19)
        
        #Close connection
        connection.close()
    
    
    
    
    
    
    def q_rating_2018():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_rating_18 = (
            "Select count(id),"
            "avg(curriculum),"
            "avg(job_support),"
            "school,"
            "count(id)/(select count(id) from main_data where  query_date BETWEEN '2019-01-01' AND '2020-01-01')*100 as share_percentage"
            "from main_data"
            "where  query_date BETWEEN '2018-01-01' AND '2019-01-01'"
            "group by school;"
            )
        
        #execute query
        cursor.execute(query_rating_18)
        
        #Close connection
        connection.close()
        
        
    
    
    def q_rating_2017():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_rating_17 = (
            "Select count(id),"
            "avg(curriculum),"
            "avg(job_support),"
            "school,"
            "count(id)/(select count(id) from main_data where  query_date BETWEEN '2019-01-01' AND '2020-01-01')*100 as share_percentage"
            "from main_data"
            "where  query_date BETWEEN '2017-01-01' AND '2018-01-01'"
            "group by school;"
            )
        
        #execute query
        cursor.execute(query_rating_17)
        
        #Close connection
        connection.close()
    
    q_rating_2017()
    q_rating_2018()
    q_rating_2019()
    
    


#/*este aqui é a linha temporal do 'market share' do ironhack*/
    

def secound_analysis():

    def q_temp_ms():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_temp_mk_share= (
            "select"
            "(select "
            "count(id)/(select count(id) from main_data where  query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2017-01-01' AND '2018-01-01' and school = 'ironhack'"
            "group by school) as '2017',"
            "(select "
            "count(id)/(select count(id) from main_data where   query_date BETWEEN '2018-01-01' AND '2019-01-01')*100 as share_percentage"
            "from main_data"
            "where  query_date BETWEEN '2018-01-01' AND '2019-01-01' and school = 'ironhack'"
            "group by school) as '2018',"
            "(select "
            "count(id)/(select count(id) from main_data where  query_date BETWEEN '2019-01-01' AND '2020-01-01')*100 as share_percentage"
            "from main_data"
            "where  query_date BETWEEN '2019-01-01' AND '2020-01-01' and school = 'ironhack'"
            "group by school) as '2019'"
            "from main_data"
            "limit 1;"
            
            
            )
        
        #execute query
        cursor.execute(query_temp_mk_share)
        
        #Close connection
        connection.close()
    
    q_temp_ms()



#/*este é o numero total de comentarios em cada ano*/
def third_analysis():

    def q_tot_com():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_tot_com = (
            "select "
            "(select count(id)"
            "from main_data"
            "where query_date BETWEEN '2017-01-01' AND '2018-01-01') as mkt2017,"
            "(select count(id)"
            "from main_data"
            "where query_date BETWEEN '2018-01-01' AND '2019-01-01') as mkt2018,"
            "(select count(id)"
            "from main_data"
            "where query_date BETWEEN '2019-01-01' AND '2020-01-01') as mkt2019"
            "from main_data "
            "limit 1;"   
            )
        
        #execute query
        cursor.execute(query_tot_com)
        
        #Close connection
        connection.close()
        
        

#-------------------percentage of anon com per year-------------------#
    



    
    def q_pc_anon_19():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_anon_19 = (
            "select count(id),"
            "avg(curriculum), "
            "avg(job_support), "
            "school,"
            "count(id)/(select count(id) from main_data where anonymous = 1  and query_date BETWEEN '2019-01-01' AND '2020-01-01')*100 as share_percentage"
            "from main_data"
            "where anonymous = 1  and query_date BETWEEN '2019-01-01' AND '2020-01-01'"
            "group by school;"
            )
        
        #execute query
        cursor.execute(query_anon_19)
        
        #Close connection
        connection.close()
    
    
    def q_pc_anon_18():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_anon_18 = (
            "select count(id),"
            "avg(curriculum), "
            "avg(job_support), "
            "school,"
            "count(id)/(select count(id) from main_data where anonymous = 1  and query_date BETWEEN '2019-01-01' AND '2020-01-01')*100 as share_percentage"
            "from main_data"
            "where anonymous = 1  and query_date BETWEEN '2018-01-01' AND '2019-01-01'"
            "group by school;"
            )
        
        #execute query
        cursor.execute(query_anon_18)
        
        #Close connection
        connection.close()
        
    def q_pc_anon_17():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_anon_17 = (
            "select count(id),"
            "avg(curriculum), "
            "avg(job_support), "
            "school,"
            "count(id)/(select count(id) from main_data where anonymous = 1  and query_date BETWEEN '2019-01-01' AND '2020-01-01')*100 as share_percentage"
            "from main_data"
            "where anonymous = 1  and query_date BETWEEN '2017-01-01' AND '2018-01-01'"
            "group by school;"
            )
        
        #execute query
        cursor.execute(query_anon_17)
        
        #Close connection
        connection.close()

    q_tot_com()
    q_pc_anon_17()
    q_pc_anon_18()
    q_pc_anon_19()
    


#-----------The ultimate table tamm tammmm tammmmm-------------#
        
  def final_market_analysis():  
    #create table "maarket"
    def create_mk_table():  
        #Connect to the database where you are storing data
        
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        
        cursor = connection.cursor()
        
        #create database struct
        query_create_market_table = ("CREATE TABLE "
        "market ("
         "ind CHAR(255),"
         "y2017 CHAR(255),"
         "y2018 CHAR(255),"
         "y2019 CHAR(255))")
        
        cursor.execute(query_create_market_table)
        
        
        
        
        connection.close()
        
        
    
    def populate_market_table():
         connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        
        cursor = connection.cursor()
        
        #create database struct
        query_populate_market_table = (
        
            "insert into market (ind,y2017,y2018,y2019) values"
            "((('total_reviews')),"
            "((select count(id)"
            "from main_data"
            "where  query_date BETWEEN '2017-01-01' AND '2018-01-01')),"
            "((select count(id)"
            "from main_data"
            "where  query_date BETWEEN '2018-01-01' AND '2019-01-01')),"
            "((select count(id)"
            "from main_data"
            "where  query_date BETWEEN '2019-01-01' AND '2020-01-01'))),"
            
            "((('ironhack_reviews')),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2017-01-01' AND '2018-01-01' and school = 'ironhack'"
            "group by school)),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2018-01-01' AND '2019-01-01' and  school = 'ironhack'" 
            "group by school)),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2019-01-01' AND '2020-01-01' and  school = 'ironhack'" 
            "group by school))),"
            
            "((('lewagon_reviews')),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2017-01-01' AND '2018-01-01' and school = 'le-wagon'" 
            "group by school)),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2018-01-01' AND '2019-01-01' and school = 'le-wagon'" 
            "group by school)),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2019-01-01' AND '2020-01-01' and school = 'le-wagon'" 
            "group by school))),"
            
            "((('hackwagon_reviews')),"
            "(('0')),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2018-01-01' AND '2019-01-01' and school = 'hackwagon-academy'"
            "group by school)),"
            "((select count(id)"
            "from main_data"
            "where query_date BETWEEN '2019-01-01' AND '2020-01-01' and school = 'hackwagon-academy'"
            "group by school))),"
            
            "((('ironhack_share')),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2017-01-01' AND '2018-01-01' and school = 'ironhack' "
            "group by school)),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2018-01-01' AND '2019-01-01' and school = 'ironhack'" 
            "group by school)),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2019-01-01' AND '2020-01-01' and school = 'ironhack'" 
            "group by school))),"
            
            "((('lewagon_share')),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2017-01-01' AND '2018-01-01' and school = 'le-wagon'" 
            "group by school)),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2018-01-01' AND '2019-01-01' and school = 'le-wagon'"
            "group by school)),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2019-01-01' AND '2020-01-01' and school = 'le-wagon'"
            "group by school))),"
            
            "((('hackwagon_share')),"
            "((0)),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2018-01-01' AND '2019-01-01' and school = 'hackwagon-academy' "
            "group by school)),"
            "((select count(id)/(select count(id) from main_data where query_date BETWEEN '2017-01-01' AND '2018-01-01')*100 as share_percentage"
            "from main_data"
            "where query_date BETWEEN '2019-01-01' AND '2020-01-01' and school = 'hackwagon-academy' "
            "group by school)));"
            
             
         )
        
        cursor.execute(query_populate_market_table)
        
        
        
        
        connection.close()
    
    
    def q_s_mk_sh():
        #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_s_mk_sh = ("SELECT * FROM market")
        
        #execute query
        cursor.execute(query_s_mk_sh)
        
        #Close connection
        connection.close()   
    
    
    create_mk_table()
    populate_market_table()
    q_s_mk_sh()
    
    #Define the connection
        connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
        
        #Create cursor
        cursor = connection.cursor()
        
        #create the query
        query_s_mk_sh = ("DROP TABLE market")
        
        #execute query
        cursor.execute(query_s_mk_sh)
        
        #Close connection
        connection.close()   
    
     

    
    
    
    
