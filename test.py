import requests
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import pymysql
import sqlalchemy

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def get_requests(api_path, **kwargs):
    response = requests.get(api_path.format(**kwargs)).json()
    df = pd.DataFrame(response)
    return df

def insert_rows(data, table,cols):
    user = 'user'
    host = '127.0.0.1'#'loacalhost'
    password = 'password'
    database = 'bet'

    print(data.head())

    tb_d = data[cols].drop_duplicates()

    engine = sqlalchemy.create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + "/" + database)

    tb_d.to_sql(table, engine, if_exists='append', chunksize=1000, index=False)



if __name__== '__main__':
    query_params= {"apiKey":"ab54d9325abed995e8c66e3f04968eed", "sport": "upcoming", "regions": "eu", "markets": "h2h"}
    endpoint="https://api.the-odds-api.com/v4/sports/{sport}/odds/?apiKey={apiKey}&regions={regions}&markets={markets}"
    sportsdata=get_requests(endpoint, apiKey="ab54d9325abed995e8c66e3f04968eed", sport= "upcoming", regions= "eu", markets="h2h")
    db_connect = create_server_connection("localhost", "user", 'password')
    create_database_query = "CREATE DATABASE IF NOT EXISTS bet"
    create_database(db_connect, create_database_query)
    create_db_connection("localhost", "user", "password", 'bet')

    #execute_query(db_connect, "DROP TABLE bet.sports")
    create_sports_table = """CREATE TABLE IF NOT EXISTS bet.sports  (
           sport_id INT NOT NULL AUTO_INCREMENT,
           sport_key VARCHAR(40) NOT NULL,
           sport_title VARCHAR(40) NOT NULL,
           primary key (sport_id)
          );
          """

    execute_query(db_connect, create_sports_table)
    insert_rows(sportsdata, "sports", ["sport_key", "sport_title"])

    #execute_query(db_connect, "DROP TABLE bet.region")
    #used this region tbale to make it easier because it usese less data
    create_region_table = """CREATE TABLE  IF NOT EXISTS bet.region (
           region_id INT NOT NULL AUTO_INCREMENT,
           region_name VARCHAR(40) NOT NULL,
           region_code VARCHAR(2) NOT NULL,
           primary key (region_id)
     );
          """

    execute_query(db_connect, create_region_table)
    #created a way to identify region by nnumbers
    insert_rows(pd.DataFrame([['United States', 'us'],['Europe','eu'],['United kingdom', 'uk'],['Australia', 'au']], columns = ['region_name', 'region_code']), "region", ['region_name', 'region_code'])

    #execute_query(db_connect, "DROP TABLE bet.teams")
    create_teams_table = """CREATE TABLE IF NOT EXISTS bet.teams  (
           team_id INT NOT NULL AUTO_INCREMENT,
           sport_key VARCHAR(40) NOT NULL,
           team_name VARCHAR(40) NOT NULL,
           primary key (team_id));
    """

    all_teams = sportsdata[['sport_key', 'home_team']].rename(columns={'home_team':'team_name'}).merge(sportsdata[['sport_key', 'away_team']].rename(columns={'away_team':'team_name'}), how='outer')
    execute_query(db_connect, create_teams_table)

    insert_rows(all_teams, "teams", ["sport_key", "team_name"])

    #execute_query(db_connect, "DROP TABLE bet.games")
    #create_games_table = """ CREATE TABLE IF NOT EXISTS bet.games  (
    """ game_id INT NOT NULL AUTO_INCREMENT,
           game_time TIMESTAMP NOT NULL,
           home_team_id  INT NOT NULL,
           away_team_id INT NOT NULL,
           primary key (game_id));
    """
    #execute_query(db_connect, create_games_table)
    #insert_rows(all_teams, "teams", ["", "team_name"])

    #execute_query(db_connect, "DROP TABLE bet.odds")
    create_odds_table = """CREATE TABLE IF NOT EXISTS bet.odds  (
           odd_id INT NOT NULL AUTO_INCREMENT,
           odds_info LONGTEXT,
           region_id INT ,
           sport_id INT,
           create_ts TIMESTAMP,
           external_id VARCHAR(40),
           sport_key VARCHAR(40) NOT NULL,
           primary key (odd_id)       
     );
          """
    execute_query(db_connect, create_odds_table)

    odds_table = sportsdata[["id","sport_key","bookmakers"]]
    odds_table['bookmakers'] = odds_table['bookmakers'].astype("string")


    for id, key, info in zip(odds_table['id'] ,odds_table['sport_key'],odds_table['bookmakers']):

        sql = """
        INSERT INTO bet.odds (external_id, sport_key,odds_info) VALUES ('{0}','{1}',"{2}");
        """.format(id, key,info)

        execute_query(db_connect, sql)

    update_Ts = """UPDATE bet.odds SET create_ts = CURRENT_TIMESTAMP() WHERE create_ts IS NULL"""
    update_region_id = """UPDATE bet.odds 
        SET region_id =  (select region_id from bet.region where region_code = '{}') WHERE region_id IS NULL""".format(query_params["regions"])

    update_sport_id = """ UPDATE bet.odds o
    inner join bet.sports s on s.sport_key = o.sport_key
        set o.sport_id = s.sport_id 
            where o.sport_id IS NULL"""

    #execute_query(db_connect, create_sports_table)
    updates=[update_Ts,update_region_id, update_sport_id]
    for updaters in updates:
        execute_query(db_connect,updaters)

    #execute_query(db_connect, update_sport_id )
