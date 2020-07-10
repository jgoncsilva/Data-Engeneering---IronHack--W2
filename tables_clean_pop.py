# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


# you must populate this dict with the schools required -> try talking to the teaching team about this

def fresh():
    schools = {   
    'ironhack' : 10828,
    'app-academy' : 10525,
    'le-wagon' : 10868,
    'general-assembly' : 10761,
    'hackwagon-academy' : 10792   
    }
    
    import re
    import pandas as pd
    from pandas.io.json import json_normalize
    import requests
    import pymysql
    
    
    def get_comments_school(school):
      TAG_RE = re.compile(r'<[^>]+>')
      # defines url to make api call to data -> dynamic with school if you want to scrape competition
      url = "https://www.switchup.org/chimera/v1/school-review-list?mainTemplate=school-review-list&path=%2Fbootcamps%2F" + school + "&isDataTarget=false&page=3&perPage=10000&simpleHtml=true&truncationLength=250"
      #makes get request and converts answer to json
      # 
      data = requests.get(url).json()
      #converts json to dataframe
      reviews =  pd.DataFrame(data['content']['reviews'])
      
      #aux function to apply regex and remove tags
      def remove_tags(x):
        return TAG_RE.sub('',x)
      reviews['review_body'] = reviews['body'].apply(remove_tags)
      reviews['school'] = school
      return reviews
    
    
    
    # could you write this as a list comprehension? ;)
    comments = []
    
    for school in schools.keys():
        print(school)
        comments.append(get_comments_school(school))
    
    comments = pd.concat(comments)
    
    
    del comments['hostProgramName']
    del comments['body']
    del comments['createdAt']
    del comments['user']
    del comments['comments']
    
    comments.rename(columns={'graduatingYear': 'graduating_year', 'queryDate': 'query_date', 'isAlumni': 'is_alumni', 'jobTitle': 'job_title', 'overallScore': 'overall_score', 'jobSupport': 'job_support'}, inplace = True)
    
    print(comments)
    
    
    
    #--------------------------#
    
    #Connect to the database where you are storing data
    
    connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
    
    #Create cursor
    
    cursor = connection.cursor()
    
    #create database struct
    query_create_main_table = ("CREATE TABLE "
    "main_data ("
     "id INT(10),"
     "name CHAR(150),"
     "anonymous CHAR(8),"
     "graduating_year INT(8),"
     "is_alumni CHAR(8),"
     "job_title CHAR(200),"
     "tagline CHAR(250),"
     "query_date DATE,"
     "program CHAR(250),"
     "overall_score FLOAT(10,2),"
     "overall FLOAT(10,2),"
     "curriculum FLOAT(10,2),"
     "job_support FLOAT(10,2),"
     "review_body TEXT(65500),"
     "school CHAR(25))")
    
    cursor.execute(query_create_main_table)
    
    
    
    
    connection.close()
    
    
    
    from sqlalchemy import create_engine
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="root1234",
                                   db="Client_IronHack"))
    
    
    # Insert whole DataFrame into MySQL
    comments.to_sql('main_data', con = engine, if_exists = 'append', chunksize = 1000000, index=False)
    
    
    #-------------------MAIN_DATA -> CREATED------------#
    
    
    from pandas.io.json import json_normalize
    
    def get_school_info(school, school_id):
        url = 'https://www.switchup.org/chimera/v1/bootcamp-data?mainTemplate=bootcamp-data%2Fdescription&path=%2Fbootcamps%2F'+ str(school) + '&isDataTarget=false&bootcampId='+ str(school_id) + '&logoTag=logo&truncationLength=250&readMoreOmission=...&readMoreText=Read%20More&readLessText=Read%20Less'
    
        data = requests.get(url).json()
    
        data.keys()
    
        courses = data['content']['courses']
        courses_df = pd.DataFrame(courses, columns= ['courses'])
    
        locations = data['content']['locations']
        locations_df = json_normalize(locations)
    
        badges_df = pd.DataFrame(data['content']['meritBadges'])
        
        website = data['content']['webaddr']
        description = data['content']['description']
        logoUrl = data['content']['logoUrl']
        school_df = pd.DataFrame([website,description,logoUrl]).T
        school_df.columns =  ['website','description','LogoUrl']
    
        locations_df['school'] = school
        courses_df['school'] = school
        badges_df['school'] = school
        school_df['school'] = school
        
        # how could you write a similar block of code to the above in order to record the school ID?
    
        locations_df['school_id'] = school_id
        courses_df['school_id'] = school_id
        badges_df['school_id'] = school_id
        school_df['school_id'] = school_id
    
        return locations_df, courses_df, badges_df, school_df
    
    locations_list = []
    courses_list = []
    badges_list = []
    schools_list = []
    
    for school, id in schools.items():
        print(school)
        a,b,c,d = get_school_info(school,id)
        
        locations_list.append(a)
        courses_list.append(b)
        badges_list.append(c)
        schools_list.append(d)
    
    #---------------#
    
    locations = pd.concat(locations_list)
    locations
    
    #Cut the fat off!
    
    del locations['description']
    del locations['country.id']
    del locations['country.abbrev']
    del locations['city.id']
    del locations['city.keyword']
    del locations['state.id']
    del locations['state.name']
    del locations['state.abbrev']
    del locations['state.keyword']
    
    #rename index
    locations.rename(columns={'country.name': 'country_name', 'city.name': 'city_name'}, inplace = True)
    
    #Connect to the database where you are storing data
    
    connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
    
    #Create cursor
    
    cursor = connection.cursor()
    
    #create database struct
    query_create_geo_table = ("CREATE TABLE "
    "geo_data ("
     "id INT(10),"
     "country_name CHAR(150),"
     "city_name CHAR(150),"
     "school CHAR(50),"
     "school_id INT(10))")
    
    cursor.execute(query_create_geo_table)
    
    
    
    
    connection.close()
    
    #send data to db
    
    from sqlalchemy import create_engine
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="root1234",
                                   db="Client_IronHack"))
    
    
    # Insert whole DataFrame into MySQL
    locations.to_sql('geo_data', con = engine, if_exists = 'append', chunksize = 1000000, index=False)
    
    #--------------GEO_DATA-> COMPLETED--------#
    
    courses = pd.concat(courses_list)
    
    del courses['school_id']
    #Connect to the database where you are storing data
    
    connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
    
    #Create cursor
    
    cursor = connection.cursor()
    
    #create database struct
    query_create_course_table = ("CREATE TABLE "
    "course_data ("
     "courses CHAR(250),"
     "school CHAR(50))")
    
    cursor.execute(query_create_course_table)
    
    
    
    connection.close()
    
    #send data to db
    
    from sqlalchemy import create_engine
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="root1234",
                                   db="Client_IronHack"))
    
    
    # Insert whole DataFrame into MySQL
    courses.to_sql('course_data', con = engine, if_exists = 'append', chunksize = 1000000, index=False)
    
    
    #----------COURSE_DATA -> COMPLETED--------#
    
    badges = pd.concat(badges_list)
    
    #Cut the fat
    
    del badges['keyword']
    del badges['school_id']
    #create db struc
    
    #Connect to the database where you are storing data
    
    connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
    
    #Create cursor
    
    cursor = connection.cursor()
    
    #create database struct
    query_create_environment_table = ("CREATE TABLE "
    "environment_data ("
     "name CHAR(150),"
     "description VARCHAR(15000),"
     "school CHAR(50))")
    
    cursor.execute(query_create_environment_table)
    
    
    
    connection.close()
    
    #send data to db
    
    from sqlalchemy import create_engine
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="root1234",
                                   db="Client_IronHack"))
    
    
    # Insert whole DataFrame into MySQL
    badges.to_sql('environment_data', con = engine, if_exists = 'append', chunksize = 1000000, index=False)
    
    
    
    #----------ENVIRONMENT_DATA -> COMPLETED-----#
    
    schools = pd.concat(schools_list)
    
    del schools['school_id']
    
    #rename index
    
    schools.rename(columns={'LogoUrl': 'logo_url'}, inplace = True)
    
    
    #Connect to the database where you are storing data
    
    connection = pymysql.connect(host='localhost', user='root', password='root1234', db='Client_IronHack')
    
    #Create cursor
    
    cursor = connection.cursor()
    
    #create database struct
    query_create_link_table = ("CREATE TABLE "
    "link_data ("
     "website CHAR(250),"
     "description VARCHAR(15000),"
     "logo_url CHAR(250),"
     "school CHAR(50))")
    
    cursor.execute(query_create_link_table)
    
    
    
    connection.close()
    
    #send data to db
    
    from sqlalchemy import create_engine
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="root1234",
                                   db="Client_IronHack"))
    
    
    # Insert whole DataFrame into MySQL
    schools.to_sql('link_data', con = engine, if_exists = 'append', chunksize = 1000000, index=False)