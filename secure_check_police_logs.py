# ======================================================================================================================================================================
#
#                                                            SECURECHECK PROJECT  
#
#                                            A PYTHON - SQL DIGITAL LEDGER FOR POLICE POST LOGS
#
#                                              PYTHON  +  POSTGRESQL  +  STREAMLIT INTEGRATION    
# ======================================================================================================================================================================
#------------------------------------------------------------import required---------------------------------------------------------------------

import pandas as pd
import sqlalchemy
import psycopg2
import streamlit as st
import plotly.express as px


#-------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------load the dataset---------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------

df =  pd.read_csv("C:/Users/Yokesh/police-ledger/traffic_stops - traffic_stops_with_vehicle_number.csv")


#-------------------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------print the data --------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------

print(df)

#-------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------check the data types-----------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------

df.info()


#-------------------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------check the nullvalues----------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------

df.isnull().sum()


#-------------------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------remove columns with all missing values------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------

df.dropna(axis=1, how='all',inplace=True)

#-------------------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------- create to database postgresql---------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------


#import psycopg2
# connection = psycopg2.connect(
#       host="localhost",
#       user="postgres",
#       password="9791243162",
#       port = 5432
#   )



# mediator = connection.cursor()

# mediator.execute("create database secure_check_db;")

# print("database 'secure_check_db' created successfully")



#-------------------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------- push to bluk data-----------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------


# engine_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"


# connection = create_engine(engine_string)

# #df =  pd.read_csv("C:/Users/Yokesh/police-ledger/traffic_stops - traffic_stops_with_vehicle_number.csv")

# df.to_sql("traffic_stops",connection,if_exists="replace",index=False)

# print("complete")

#-------------------------------------------------------------------------------------------------------------------------------------------------
# -------------------------------------connecting to securecheck database & build interactive streamlit dashboard---------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------

host="localhost"
username="postgres"
password="9791243162"
port = 5432
database = "secure_check_db"


import sqlalchemy
from sqlalchemy import create_engine
engine =  create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")


def load_data():
    query = 'select * from traffic_stops'
    df = pd.read_sql(query,engine)
    return df


st.title("🚔securecheck: police check post digital ledger")
st.markdown("📷Actionable Insights for Every Police Checkpoint")

data = load_data()
st.dataframe(data)



st.set_page_config(page_title="patrol Intelligence Dashboard",layout="wide")


#-------------------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------key metrics--------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------------------
st.header("📊 Strategic Metrics ")

col1, col2, col3, col4, col5 = st.columns(5 )

with col1:
    total_stops = data.shape[0]
    st.metric("Total police stops", total_stops)

with col2:
    arrests = data[data['stop_outcome'].str.contains("arrest", case=False, na=False)].shape[0]
    st.metric("Total Arrests", arrests)

with col3:
    warnings = data[data['stop_outcome'].str.contains("warning", case=False, na=False)].shape[0]
    st.metric("Total warnings",warnings)

with col4:
    top_violation = data['violation_raw'].value_counts().idxmax()
    top_violation_count = data['violation_raw'].value_counts().max()
    st.metric(f" Highest violation: {top_violation}",top_violation_count)

with col5:
    drug_related =data[data['drugs_related_stop']==1].shape[0]
    st.metric("Drug related stops", drug_related)




st.header("📈 Visual Insights")

tab1, tab2, tab3 = st.tabs(["📊 Violations", "🧁 Gender", "📦 Age Distribution"])

# --------------------------------------------------------------------------------------------------------------------------------------------------
# 📊----------------------------------------------- Tab 1: Top Violation Types (Bar Chart)---------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------------------------
with tab1:
    if not data.empty and 'violation_raw' in data.columns:
        violation_data = data['violation_raw'].value_counts().reset_index()
        violation_data.columns = ['Violation', 'Count']

        fig = px.bar(
            violation_data.head(5),
            x='Violation',
            y='Count',
            color='Violation',
            title="Top 5 Violation Types",
            text='Count'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No traffic violations found.")

# ------------------------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------🧁 Tab 2: Gender Distribution (Pie Chart)----------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------------------
with tab2:
    if not data.empty and 'driver_gender' in data.columns:
        gender_data = data['driver_gender'].value_counts().reset_index()
        gender_data.columns = ['Gender', 'Count']

        fig = px.pie(
            gender_data,
            names='Gender',
            values='Count',
            title="Driver Gender Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No records found for gender anaysis.")

# ------------------------------------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------📦 Tab 3: Driver Age by Violation (Box Plot)-----------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------------------------------
with tab3:
    if 'driver_age' in data.columns and 'violation_raw' in data.columns:
        clean_data = data.dropna(subset=['driver_age', 'violation_raw'])
        clean_data['driver_age'] = pd.to_numeric(clean_data['driver_age'], errors='coerce')

        top_violations = clean_data['violation_raw'].value_counts().head(5).index
        filtered = clean_data[clean_data['violation_raw'].isin(top_violations)]

        fig = px.box(
            filtered,
            x='violation_raw',
            y='driver_age',
            color='violation_raw',
            title="Driver Age Distribution by Violation Type",
            labels={'violation_raw': 'Violation Type', 'driver_age': 'Driver Age'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No age-realted records to visualize.")


st.markdown("---------")

st.header("🚦⚠️🛑 Medium level")
medium_query = st.selectbox("Execute a database query",[

    "",
    "Top 10 vehicle_Number involved in drug-realted stops",
    "vehicles most frequently searched",
    "Driver age group with highest arrest rate",
    "Gender distribution of drivers stopped in each country",
    "Race and gender combination with the highest search rate",
    "What time of day sees the most traffic stops",
    "What is the average stop duration for different violations",
    "Stops during the night more likely to lead to arrests",
    "Which violations are most associated with searches or arrests",
    "Which violations are most common among younger driver(<25)",
    "Is there a violation that rarely results in search or arrests",
    "Which countries report the highest rate of drug-related stops",
    " What is the arrest rate by country and violation",
    "Which country has the most stops with search conducted"
])


medium_level_queries = {
"Top 10 vehicle_Number involved in drug-realted stops":
 """
SELECT vehicle_number, COUNT(*) AS drugs_related_stop_count
FROM traffic_stops
WHERE drugs_related_stop = TRUE
GROUP BY vehicle_number
ORDER BY drugs_related_stop_count DESC
LIMIT 10;""",


"vehicles most frequently searched": """
SELECT vehicle_number, COUNT(*) AS search_count
FROM traffic_stops
WHERE search_conducted = TRUE
GROUP BY vehicle_number
ORDER BY search_count DESC
LIMIT 10;""",

"Driver age group with highest arrest rate":"""
SELECT driver_age, 
       COUNT(*) FILTER (WHERE is_arrested = TRUE)::float / COUNT(*) AS arrest_rate
FROM traffic_stops
GROUP BY driver_age
ORDER BY arrest_rate DESC
LIMIT 10;""",

"Gender distribution of drivers stopped in each country":"""
SELECT country_name, driver_gender, COUNT(*) AS count
FROM traffic_stops
GROUP BY country_name, driver_gender
ORDER BY country_name, driver_gender;""",

"Race and gender combination with the highest search rate":"""
SELECT driver_race, driver_gender,
       COUNT(*) FILTER (WHERE search_conducted = TRUE)::float / COUNT(*) AS search_rate
FROM traffic_stops
GROUP BY driver_race, driver_gender
ORDER BY search_rate DESC
LIMIT 5;""",

"What time of day sees the most traffic stops":"""
SELECT EXTRACT(HOUR FROM stop_time::time) AS hour, COUNT(*) AS stop_count
FROM traffic_stops
GROUP BY hour
ORDER BY stop_count DESC
LIMIT 20;""",


"What is the average stop duration for different violations":"""

SELECT violation, AVG(NULLIF(stop_duration, '')::FLOAT8) AS avg_stop_duration
FROM traffic_stops
WHERE stop_duration ~ '^[15]+(\.[30]+)?$'
GROUP BY violation
ORDER BY avg_stop_duration DESC;""",


"Stops during the night more likely to lead to arrests":"""
SELECT
  CASE 
    WHEN EXTRACT(HOUR FROM stop_time::time) BETWEEN 20 AND 23 OR EXTRACT(HOUR FROM stop_time::time) BETWEEN 0 AND 5 THEN 'Night'
    ELSE 'Day'
  END AS time_period,
  COUNT(*) FILTER (WHERE is_arrested = TRUE)::float / COUNT(*) AS arrest_rate
FROM traffic_stops
GROUP BY time_period;""",



"Which violations are most associated with searches or arrests":"""
SELECT violation,
       COUNT(*) FILTER (WHERE search_conducted = TRUE) AS search_count,
       COUNT(*) FILTER (WHERE is_arrested = TRUE) AS arrest_count
FROM traffic_stops
GROUP BY violation
ORDER BY search_count DESC, arrest_count DESC;""",

"Which violations are most common among younger driver(<25)":"""
SELECT violation, COUNT(*) AS count
FROM traffic_stops
WHERE driver_age < 25
GROUP BY violation
ORDER BY count DESC
LIMIT 5;""",

"Is there a violation that rarely results in search or arrests":"""
SELECT violation,
       COUNT(*) FILTER (WHERE is_arrested = TRUE)::float / COUNT(*) AS arrest_rate
FROM traffic_stops
GROUP BY violation
ORDER BY arrest_rate ASC
LIMIT 5;""",

"Which countries report the highest rate of drug-related stops":"""
SELECT country_name,
       COUNT(*) FILTER (WHERE drugs_related_stop = TRUE)::float / COUNT(*) AS drug_stop_rate
FROM traffic_stops
GROUP BY country_name
ORDER BY drug_stop_rate DESC;""",


" What is the arrest rate by country and violation":"""
SELECT country_name, violation,
       COUNT(*) FILTER (WHERE is_arrested = TRUE)::float / COUNT(*) AS arrest_rate
FROM traffic_stops
GROUP BY country_name, violation
ORDER BY arrest_rate DESC;""",

"Which country has the most stops with search conducted":"""
SELECT country_name, COUNT(*) AS search_count
FROM traffic_stops
WHERE search_conducted = TRUE
GROUP BY country_name
ORDER BY search_count DESC
LIMIT 5;"""

}   


st.header(" 🧠Complex")
complex_query =st.selectbox("Execute a database query",[
    
    "",
    "Yearly breakdown of stops and arrests by country ",
    "Driver violation trends based on age and race ",
    "Time period analysis of stops ",
    "Violations with high search and Arrest Rates",
    "Driver demographics by country ",
    "Top 5 violations with highest arrest Rates"
])


complex_level_queries ={
"Yearly breakdown of stops and arrests by country ":"""
SELECT 
    EXTRACT(YEAR FROM stop_date::DATE) AS year,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(
        100.0 * SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) / COUNT(*), 2
    ) AS arrest_rate_percent,
    RANK() OVER (
        PARTITION BY EXTRACT(YEAR FROM stop_date::DATE) 
        ORDER BY SUM(CASE WHEN is_arrested THEN 1 ELSE 0 END) DESC
    ) AS rank_by_arrest
FROM traffic_stops
GROUP BY year
ORDER BY year, rank_by_arrest;""",


"Driver violation trends based on age and race ":"""
WITH age_grouped AS (
    SELECT *,
        CASE 
            WHEN driver_age < 18 THEN 'Under 18'
            WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
            WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
            WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
            ELSE '60+' 
        END AS age_group
    FROM traffic_stops
)
SELECT 
    age_group,
    driver_race,
    violation,
    COUNT(*) AS violation_count
FROM age_grouped
GROUP BY age_group, driver_race, violation
ORDER BY age_group, driver_race, violation_count DESC""",

"Time period analysis of stops ":"""
SELECT 
    EXTRACT(YEAR FROM stop_date::date) AS year,
    TO_CHAR(stop_date::date, 'Month') AS month,
    EXTRACT(HOUR FROM stop_time::time) AS hour_of_day,
    COUNT(*) AS stop_count
FROM traffic_stops
GROUP BY year, month, hour_of_day
ORDER BY year, month, hour_of_day;""",

"Violations with high search and Arrest Rates":"""
SELECT 
    violation,
    COUNT(*) AS total_stops,
    COUNT(*) FILTER (WHERE search_conducted = TRUE) AS search_count,
    COUNT(*) FILTER (WHERE is_arrested = TRUE) AS arrest_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE search_conducted = TRUE) / COUNT(*), 2) AS search_rate_percent,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_arrested = TRUE) / COUNT(*), 2) AS arrest_rate_percent,
    RANK() OVER (ORDER BY COUNT(*) FILTER (WHERE search_conducted = TRUE) DESC) AS rank_by_search,
    RANK() OVER (ORDER BY COUNT(*) FILTER (WHERE is_arrested = TRUE) DESC) AS rank_by_arrest
FROM traffic_stops
GROUP BY violation
ORDER BY search_rate_percent DESC, arrest_rate_percent DESC;""",

"Driver demographics by country ":"""
SELECT 
    country_name,
    COUNT(*) FILTER (WHERE driver_gender = 'M') AS male_count,
    COUNT(*) FILTER (WHERE driver_gender = 'F') AS female_count,
    driver_race,
    COUNT(*) AS total_by_race
FROM traffic_stops
GROUP BY country_name, driver_race
ORDER BY country_name, driver_race;""",

"Top 5 violations with highest arrest Rates":"""
SELECT 
    violation,
    COUNT(*) AS total_stops,
    COUNT(*) FILTER (WHERE is_arrested = TRUE) AS arrest_count,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_arrested = TRUE) / COUNT(*), 2) AS arrest_rate_percent
FROM traffic_stops
GROUP BY violation
HAVING COUNT(*) > 10 -- Optional filter to remove rare violation
ORDER BY arrest_rate_percent DESC
LIMIT 5;"""
    
}



if st.button("Run Query"):
    query_to_run =" "

    if medium_query and medium_query in medium_level_queries:
        query_to_run =medium_level_queries[medium_query]
    elif complex_query and complex_query in complex_level_queries:
        query_to_run =complex_level_queries[complex_query]

    if query_to_run:
        df =pd.read_sql(query_to_run,engine)
        st.success("Query executed successfully!")
        st.dataframe(df)
    else:
        st.warning("please select a valid query.")


st.markdown("---------")
st.markdown("Innovating Law Enforcement - Powered by Securecheck")


st.header("Law-Enforcement-Optimized NLP Filter")
st.markdown("Provide the required details to generate a data driven prediction of the stop result")


st.header("Log New Incident & Analyze Predicted Outcome and Infraction")
with st.form("Incident_Log_form"):
    stop_date = st.date_input("Stop Date")
    stop_time = st.time_input("Stop Time")
    country_name = st.text_input("Country Name")
    driver_gender = st.selectbox("Driver Gender", ["male", "Female"])
    driver_age = st.number_input("Driver Age", min_value=16, max_value=100,value=27)
    driver_race= st.text_input("Driver Race")
