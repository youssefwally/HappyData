import requests
import json

import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns 
import re
from functools import reduce 
import requests
from bs4 import BeautifulSoup
from sklearn.preprocessing import MinMaxScaler
from sklearn import preprocessing
from scipy import stats
import warnings
warnings.simplefilter('ignore')

from airflow import DAG
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

import pandas as pd

##################################################################################
# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 25)
    }

##################################################################################
# step 3 - instantiate DAG

dag = DAG(
    'pipeline-dag',
    default_args=default_args,
    description='Fetch covid data from API',
    schedule_interval='@once',
)

##################################################################################
# step 4 Define tasks

def unique(list1): 
    x = np.array(list1) 
    x = np.unique(x) 
    return x

############################### CLEAN COUNTRY NAMES ##############################
def Clean_Country_Names(df):
    extract_string_before_brackets = re.compile("(.*?)\s*\((.*?)\)")
    extract_string_before_comma = re.compile("(.*?)\s*\,")
    for x in df.index:
        country = df['Country/Region'][x]
        if '&' in country:
            df['Country/Region'][x] = country.replace('&','and')
        if(country == "Viet Nam"):
            country = "Vietnam"
            df['Country/Region'][x] = country
            continue
        if(country == "Hong Kong S.A.R., China"):
            country = "Hong Kong"
            df['Country/Region'][x] = country
            continue
        if(country == "United States of America" or country == "America" or country == "United-states"):
            country = "United States"
            df['Country/Region'][x] = country
            continue
        if(country == "The former Yugoslav republic of Macedonia" or country =="North Macedonia"):
            country = "Macedonia"
            df['Country/Region'][x] = country
            continue
        if(country == "Taiwan Province of China"):
            country = "Taiwan"
            df['Country/Region'][x] = country
            continue
        if(country == "Syrian Arab Republic"):
            country = "Syria"
            df['Country/Region'][x] = country
            continue
            
        if(country == "Democratic People's Republic of Korea" or country == "North-korea"):
            country = "North Korea"
            df['Country/Region'][x] = country
            continue
            
        if(country == "San-marino"):
            country = "San Marino"
            df['Country/Region'][x] = country
            continue
            
        if(country == "Congo" or country == "Congo-rep"):
            country = 'Rep. Congo'
            df['Country/Region'][x] = country
            
        if(country == "Republic of Korea" or country =="Korea" or country == "Republic of Korea " or country == "South-korea"):
            country = "South Korea"
            df['Country/Region'][x] = country
            continue
        if(country == "Democratic Republic of the Congo" or country == "Congo-dem-rep"):
            country = "Dem. Congo"
            df['Country/Region'][x] = country
            continue
        if(country == "Russian Federation"):
            country = "Russia"
            df['Country/Region'][x] = country
            continue
        if(country == "North-macedonia"):
            country = 'Macedonia'
            df['Country/Region'][x] = country
            continue
        if(country == "South-sudan"):
            country = "South Sudan"
            df['Country/Region'][x] = country
            continue
        if(country == "Somaliland region"):
            country = "Somaliland Region"
            df['Country/Region'][x] = country
            continue
        if(country == "United Kingdom of Great Britain and Northern Ireland" or country == "United-kingdom"):
            country = "United Kingdom"
            df['Country/Region'][x] = country
            continue
        if(country == "United Republic of Tanzania"):
            country = "Tanzania"
            df['Country/Region'][x] = country
            continue
        if(country == "Czech Republic" or country == "Czech-republic"):
            country = "Czechia"
            df['Country/Region'][x] = country
            continue
        if(country == "Palestinian Territories"):
            country = "Palestine"
            df['Country/Region'][x] = country
            continue
        if(country == "Northern Cyprus"):
            df['Country/Region'][x] = "North Cyprus"
            continue
        if(country == "Syrian-arab-republic"):
            country = "Syria"
            df['Country/Region'][x] = country
            continue
            
        if(country == "lao people's democratic republic" or 
           country== "Lao People's Democratic Republic" or 
           country == "Lao-pdr"):
            df['Country/Region'][x] = "Laos"
            continue
        if(country == "Kyrgyz-republic"):
            country = "Kyrgyzstan"
            df['Country/Region'][x] = country
            continue
        if(country == "Slovak-republic"):
            country = "Slovakia"
            df['Country/Region'][x] = country
            continue
        if(country == "St-vincent-and-the-grenadines"):
            country = "Saint Vincent and the Grenadines"
            df['Country/Region'][x] = country
            continue
        if(country =="St-lucia"):
            country = "Saint Lucia"
            df['Country/Region'][x] = country
            continue
        if(country =="St-kitts-and-nevis"):
            country = "Saint Kitts and Nevis"
            df['Country/Region'][x] = country
            continue
        if(country =="Sao-tome-and-principe"):
            country = "Sao Tome and Principe"
            df['Country/Region'][x] = country
            continue
        if(country == "Papua-new-guinea"):
            country= "Papua New Guinea"
            df['Country/Region'][x] = country
            continue
            

        if '(' in country:
            extract = extract_string_before_brackets.match(country)
            if extract.group(2) == "U.S.":
                df['Country/Region'][x] = "United States"
            elif  extract.group(2) == "British":
                df['Country/Region'][x] = "United Kingdom"
            else:
                df['Country/Region'][x] = extract.group(1)
            continue
        if ',' in country:
            df['Country/Region'][x] =  extract_string_before_comma.match(country).group(1)
            continue
        if 'Republic of 'in country:
            df['Country/Region'][x] = country.replace('Republic of ','')
###############################################################################################
def unique(list1):
    x = np.array(list1)
    x = np.unique(x)
    return x

###############################################################################################
def isNan(string):
    return string != string
###############################################################################################

def Replace_NAN_With_Group_Mean(df, column_names, groupby_name):
    for column_name in column_names:
        df[column_name] = df.groupby(groupby_name)[column_name].transform(
            lambda x: x.fillna(x.mean()))
###############################################################################################

def Replace_NAN_With_Mean(df, column_name):
    df[column_name].fillna(df[column_name].mean(), inplace=True)
###############################################################################################
def normalise_and_scale(df, columns):
    df_def = df.copy()
    for i in columns:
        if(df_def[i].min() < 1):
            df_def[i] = df_def[i]+0.1
        df_def[i] = stats.boxcox(df_def[i])[0]
    mms = MinMaxScaler(feature_range=(0, 1))
    df_def[columns] = mms.fit_transform(df_def[columns])
    return df_def
###############################################################################################

def normalise(df, columns):
    df_def = df.copy()
    mms = MinMaxScaler(feature_range=(0, 1))
    df_def[columns] = mms.fit_transform(df_def[columns])
    return df_def
###############################################################################################
def Remove_Percentage_Year(df):
    extract_string_before_percentage = re.compile("(.*?)\s*\%")
    for x in df.index:
        growth = df["Real_Growth_Rating_Percentage"][x]
        litercy = df["Literacy_Rate_Percentage"][x]
        infiliation = df["Inflation_Percentage"][x]
        unemployment = df["Unemployment_Percentage"][x]
        if((not isNan(growth))):
            growth = re.findall('\d*\.\d+|\d+', growth)
            if growth:
                df["Real_Growth_Rating_Percentage"][x] = growth[0]
            else:
                df["Real_Growth_Rating_Percentage"][x] = np.nan

        if((not isNan(litercy))):
            litercy = re.findall('\d*\.\d+|\d+', litercy)
            if litercy:
                df["Literacy_Rate_Percentage"][x] = litercy[0]
            else:
                df["Literacy_Rate_Percentage"][x] = np.nan

        if((not isNan(infiliation))):
            infiliation = re.findall('\d*\.\d+|\d+', infiliation)
            if infiliation:
                df["Inflation_Percentage"][x] = infiliation[0]
            else:
                df["Inflation_Percentage"][x] = np.nan
        if((not isNan(unemployment))):
            unemployment = re.findall('\d*\.\d+|\d+', unemployment)
            if unemployment:
                df["Unemployment_Percentage"][x] = unemployment[0]
            else:
                df["Unemployment_Percentage"][x] = np.nan
    df["Real_Growth_Rating_Percentage"] = df.Real_Growth_Rating_Percentage.astype(
        float)
    df["Literacy_Rate_Percentage"] = df.Literacy_Rate_Percentage.astype(float)
    df["Inflation_Percentage"] = df.Inflation_Percentage.astype(float)
    df["Unemployment_Percentage"] = df.Unemployment_Percentage.astype(float)

################################## WEBSCRAPING #####################################

#################################### LANDLOCKED ###############################################
def get_landlocked_countries():
    res = []
    result = requests.get("https://en.wikipedia.org/wiki/Landlocked_country")
    src = result.content
    soup = BeautifulSoup(src, 'lxml')
    table = soup.find('table', class_ = "sortable wikitable")
    rows = table.find_all('td')
    
    for i in range(len(rows)):
        current_data = rows[i].find('span', class_ = "flagicon")
        if( current_data is not None):
            name = current_data.findNext('a').text
            res.append(name)

    return res

def webscraping_landlocked():
    landlocked_countries = get_landlocked_countries()
    landlocked_countries_cleaned = pd.DataFrame(landlocked_countries, columns = ['Country/Region'])
    Clean_Country_Names(landlocked_countries_cleaned)
    return landlocked_countries_cleaned

def add_landlocked(Country_Data_250_df):
    landlocked_countries_cleaned = webscraping_landlocked()
    landlocked_250 = Country_Data_250_df.copy()
    landlocked_250['Landlocked'] = 0
    for x in landlocked_250.index:
        country = landlocked_250['Country/Region'][x]
        if country in landlocked_countries_cleaned['Country/Region'].values:
            landlocked_250['Landlocked'][x] = 1
    return landlocked_250

############################### GDP PER CAPITA & UNEMP. RATE ######################################
def scrape_macrotrends(urls):
    per_capita = []
    for url in urls:
        data=[]
        country_name = url.split('/')[5].capitalize()
        #print(country_name)
        for year in range(2000,2016):
            data.append((country_name,year, np.nan))
        
        result = requests.get(url)
        src = result.content
        soup = BeautifulSoup(src, 'html.parser')
        script = soup.find('script', text=lambda text: text and "var chartData" in text)
        s = str(script).split(']')[0]
        
        if(s[29:][0:3] != 'ull'):
            country_per_capita_history = pd.read_json(s[29:],  lines=True)
            for year in range(len(country_per_capita_history['date'])):
                date_year = int(str(country_per_capita_history['date'][year]).split('-')[0])
                if(2000<= date_year <= 2015):
                    data[date_year%2000] = (country_name, date_year,country_per_capita_history['v1'][year])
        
        #print(url)
        #print(data)
        #print(data)
        per_capita.append(data)
        
    return per_capita

def get_urls(x):
    countries = pd.read_json("https://www.macrotrends.net/assets/php/global_metrics/country_search_list.php")
    urls = []
    for country in countries['s']:
        prms = country.split('/')
        url = "https://www.macrotrends.net/countries/" + prms[1] + "/" + prms[0] + "/" + x
        urls.append(url)
    return urls   

def adding_gdp_cap(gdp_per_capita, Life_Expectancy_Data_df):
    gdp_per_capita_df = pd.DataFrame(columns=["Country/Region", "Year", "GDP"])
    for country in gdp_per_capita:
        for entry in country:
            gdp_per_capita_df = gdp_per_capita_df.append({'Country/Region': entry[0], 'Year': entry[1], 'GDP': entry[2]}, ignore_index = True)

    for row in Life_Expectancy_Data_df.index:
        if(isNan(Life_Expectancy_Data_df['GDP'][row])):
            name = Life_Expectancy_Data_df.loc[row]['Country/Region']
            year = Life_Expectancy_Data_df.loc[row]['Year']
            if name in gdp_per_capita_df['Country/Region'].values and year in gdp_per_capita_df['Year'].values: 
                x = gdp_per_capita_df.loc[(gdp_per_capita_df['Country/Region'] == name) & (gdp_per_capita_df['Year'] == year)]
                Life_Expectancy_Data_df['GDP'][row] = x['GDP']
    return Life_Expectancy_Data_df

#################################### WORLD HAPPINESS CLEANING ###########################
def WHR_2015(world_happiness_report_2015_df):
    world_happiness_report_2015_df = world_happiness_report_2015_df.reset_index()

    world_happiness_report_2015_df = world_happiness_report_2015_df.drop('Region', axis = 1)
    world_happiness_report_2015_df = world_happiness_report_2015_df.drop('Standard Error', axis = 1)

    world_happiness_report_2015_df.rename(columns = {'Happiness Rank':'Happiness_Rank','Country': 'Country/Region', 'Economy (GDP per Capita)': 'GDP_Rating', 'Family': 'Social_Support', 'Health (Life Expectancy)': 'Health', 'Trust (Government Corruption)': 'Corruption', 'Dystopia Residual':'Dystopia_Residual','Happiness Score':'Happiness_Score'}, inplace = True)

    columns_titles = ['Happiness_Rank','Country/Region', 'Happiness_Score', 'GDP_Rating', 'Social_Support', 'Health', 'Freedom', 'Generosity', 'Corruption','Dystopia_Residual','Year']
    world_happiness_report_2015_df=world_happiness_report_2015_df.reindex(columns=columns_titles)

    return world_happiness_report_2015_df
##########
def WHR_2016(world_happiness_report_2016_df):
    world_happiness_report_2016_df = world_happiness_report_2016_df.reset_index()

    world_happiness_report_2016_df = world_happiness_report_2016_df.drop('Region', axis = 1)
    world_happiness_report_2016_df = world_happiness_report_2016_df.drop('Lower Confidence Interval', axis = 1)
    world_happiness_report_2016_df = world_happiness_report_2016_df.drop('Upper Confidence Interval', axis = 1)

    world_happiness_report_2016_df.rename(columns = {'Happiness Rank':'Happiness_Rank','Country': 'Country/Region', 'Economy (GDP per Capita)': 'GDP_Rating', 'Family': 'Social_Support', 'Health (Life Expectancy)': 'Health', 'Trust (Government Corruption)': 'Corruption', 'Dystopia Residual':'Dystopia_Residual','Happiness Score':'Happiness_Score'}, inplace = True)


    columns_titles = ['Happiness_Rank','Country/Region', 'Happiness_Score', 'GDP_Rating', 'Social_Support', 'Health', 'Freedom', 'Generosity', 'Corruption','Dystopia_Residual','Year']
    world_happiness_report_2016_df=world_happiness_report_2016_df.reindex(columns=columns_titles)

    return world_happiness_report_2016_df
##########
def WHR_2017(world_happiness_report_2017_df):
    world_happiness_report_2017_df.rename(columns = {'Happiness.Rank': 'Happiness_Rank'}, inplace = True)
    world_happiness_report_2017_df = world_happiness_report_2017_df.reset_index()

    world_happiness_report_2017_df = world_happiness_report_2017_df.drop('Whisker.high', axis = 1)
    world_happiness_report_2017_df = world_happiness_report_2017_df.drop('Whisker.low', axis = 1)

    world_happiness_report_2017_df.rename(columns = {'Country': 'Country/Region', 'Happiness.Score': 'Happiness_Score', 'Economy..GDP.per.Capita.': 'GDP_Rating', 'Family': 'Social_Support', 'Health..Life.Expectancy.': 'Health', 'Trust..Government.Corruption.': 'Corruption', 'Dystopia.Residual': 'Dystopia_Residual','Happiness Score':'Happiness_Score'}, inplace = True)

    columns_titles = ['Happiness_Rank','Country/Region', 'Happiness_Score', 'GDP_Rating', 'Social_Support', 'Health', 'Freedom', 'Generosity', 'Corruption','Dystopia_Residual','Year']
    world_happiness_report_2017_df=world_happiness_report_2017_df.reindex(columns=columns_titles)

    return world_happiness_report_2017_df
##########
def WHR_2018(world_happiness_report_2018_df):
    world_happiness_report_2018_df = world_happiness_report_2018_df.reset_index()

    world_happiness_report_2018_df['Dystopia_Residual'] = 0.00000
    for x in world_happiness_report_2018_df.index:
       world_happiness_report_2018_df['Dystopia_Residual'][x] = (world_happiness_report_2018_df['Score'][x]-(world_happiness_report_2018_df['GDP per capita'][x]+world_happiness_report_2018_df['Social support'][x]+world_happiness_report_2018_df['Healthy life expectancy'][x]+world_happiness_report_2018_df['Freedom to make life choices'][x]+world_happiness_report_2018_df['Generosity'][x]+world_happiness_report_2018_df['Perceptions of corruption'][x]))

    world_happiness_report_2018_df.rename(columns = {'Overall rank': 'Happiness_Rank', 'Country or region': 'Country/Region', 'Score': 'Happiness_Score','GDP per capita': 'GDP_Rating', 'Social support': 'Social_Support', 'Healthy life expectancy': 'Health', 'Freedom to make life choices': 'Freedom','Perceptions of corruption': 'Corruption','Happiness Score':'Happiness_Score'}, inplace = True)

    columns_titles = ['Happiness_Rank','Country/Region', 'Happiness_Score', 'GDP_Rating', 'Social_Support', 'Health', 'Freedom', 'Generosity', 'Corruption','Dystopia_Residual','Year']
    world_happiness_report_2018_df=world_happiness_report_2018_df.reindex(columns=columns_titles)

    return world_happiness_report_2018_df
##########
def WHR_2019(world_happiness_report_2019_df):
    world_happiness_report_2019_df = world_happiness_report_2019_df.reset_index()
    print(world_happiness_report_2019_df.info())
    world_happiness_report_2019_df['Dystopia_Residual'] = 0.00000
    for x in world_happiness_report_2019_df.index:
        world_happiness_report_2019_df['Dystopia_Residual'][x] = (world_happiness_report_2019_df['Score'][x]-(world_happiness_report_2019_df['GDP per capita'][x]+world_happiness_report_2019_df['Social support'][x]+world_happiness_report_2019_df['Healthy life expectancy'][x]+world_happiness_report_2019_df['Freedom to make life choices'][x]+world_happiness_report_2019_df['Generosity'][x]+world_happiness_report_2019_df['Perceptions of corruption'][x]))

    world_happiness_report_2019_df.rename(columns = {'Overall rank': 'Happiness_Rank', 'Country or region': 'Country/Region', 'Score': 'Happiness_Score','GDP per capita': 'GDP_Rating','Social support': 'Social_Support','Healthy life expectancy': 'Health','Freedom to make life choices': 'Freedom','Perceptions of corruption': 'Corruption'}, inplace = True)

    columns_titles = ['Happiness_Rank','Country/Region', 'Happiness_Score', 'GDP_Rating', 'Social_Support', 'Health', 'Freedom', 'Generosity', 'Corruption','Dystopia_Residual','Year']
    world_happiness_report_2019_df=world_happiness_report_2019_df.reindex(columns=columns_titles)

    return world_happiness_report_2019_df

################################## 250 Country Data #####################################
def country_data(Country_Data_250_df):
    Country_Data_250_df.index.names = ['Rank']

    Country_Data_250_df.rename(columns={'name': 'Country/Region', 'population': 'Population', 'area': "Area", 'gini': 'Gini', "Literacy Rate(%)": "Literacy_Rate_Percentage",
                                    'Real Growth Rating(%)': 'Real_Growth_Rating_Percentage', 'Inflation(%)': 'Inflation_Percentage', 'Unemployement(%)': 'Unemployment_Percentage'}, inplace=True)
    # Clean the data by removing the strings
    Remove_Percentage_Year(Country_Data_250_df)
    # fill nans with the mean of each region
    print(Country_Data_250_df.region.unique().size)
    print(Country_Data_250_df.subregion.unique().size)
    Country_Data_250_df.dropna(subset=['subregion'],inplace = True)
    Replace_NAN_With_Group_Mean(Country_Data_250_df,['Real_Growth_Rating_Percentage','Literacy_Rate_Percentage','Inflation_Percentage','Unemployment_Percentage'],'subregion')
    Country_Data_250_df.drop(['region','subregion','Gini','Area'], axis = 1,inplace = True) 
    return Country_Data_250_df

################################ Life Expectancy Data ####################################
def life_expectancy(Life_Expectancy_Data_df):
    Life_Expectancy_Data_df = load_data('/home/omar/airflow/dags/data/Life Expectancy Data.csv')
    Life_Expectancy_Data_df = Life_Expectancy_Data_df.reset_index()
    Life_Expectancy_Data_df.rename(columns = {'Country': 'Country/Region','infant deaths': 'Infant_Deaths','Life expectancy ':'Life_Expectancy','percentage expenditure':'Percentage_Expenditure','Measles ':'Measles', ' BMI ':'BMI','under-five deaths ':'Under-Five_Deaths','Total expenditure':'Total_Expenditure','Diphtheria ':'Diphtheria', ' HIV/AIDS':'HIV/AIDS','Adult Mortality':'Adult_Mortality',' thinness  1-19 years':'Thinness_1-19_Years',' thinness 5-9 years':'Thinness_5-9_Years','Income composition of resources':'Income_Composition_of_Resources',"Hepatitis B": 'Hepatitis_B'}, inplace = True)

    Replace_NAN_With_Group_Mean(Life_Expectancy_Data_df,['Hepatitis_B','Polio','Diphtheria'],'Country/Region')

    Life_Expectancy_Data_df["Vaccination"] = Life_Expectancy_Data_df["Hepatitis_B"] + Life_Expectancy_Data_df["Polio"] + Life_Expectancy_Data_df["Diphtheria"] 

    Life_Expectancy_Data_df.drop(['Hepatitis_B','Polio','Diphtheria','Measles','Thinness_1-19_Years', 'Thinness_5-9_Years','Under-Five_Deaths','Population', 'HIV/AIDS'], axis = 1,inplace = True)

    return Life_Expectancy_Data_df

####################################

#################################### MAIN LOAD ##########################################
def load_data(path):
    return pd.read_csv(path, index_col=0) 

def load_main_dfs():
    world_happiness_report_2015_df = load_data('/home/omar/airflow/dags/data/Happiness_Dataset/2015.csv')
    world_happiness_report_2016_df = load_data('/home/omar/airflow/dags/data/Happiness_Dataset/2016.csv')
    world_happiness_report_2017_df = load_data('/home/omar/airflow/dags/data/Happiness_Dataset/2017.csv')
    world_happiness_report_2018_df = load_data('/home/omar/airflow/dags/data/Happiness_Dataset/2018.csv')
    world_happiness_report_2019_df = load_data('/home/omar/airflow/dags/data/Happiness_Dataset/2019.csv')
    
    world_happiness_report_2015_df['Year'] = 2015
    world_happiness_report_2016_df['Year'] = 2016
    world_happiness_report_2017_df['Year'] = 2017
    world_happiness_report_2018_df['Year'] = 2018
    world_happiness_report_2019_df['Year'] = 2019

    Country_Data_250_df = load_data('/home/omar/airflow/dags/data/250 Country Data.csv')
    Life_Expectancy_Data_df = load_data('/home/omar/airflow/dags/data/Life Expectancy Data.csv')

    gdp_urls = get_urls('gdp-per-capita')
    gdp_per_capita = scrape_macrotrends(gdp_urls)


    #unemployment_urls = get_urls('unemployment-rate')
    #unemployment = scrape_macrotrends(unemployment_urls)

    world_happiness_report_2015_df = WHR_2015(world_happiness_report_2015_df)
    world_happiness_report_2016_df = WHR_2016(world_happiness_report_2016_df)
    world_happiness_report_2017_df = WHR_2017(world_happiness_report_2017_df)
    world_happiness_report_2018_df = WHR_2018(world_happiness_report_2018_df)
    world_happiness_report_2019_df = WHR_2019(world_happiness_report_2019_df)

    frames = [world_happiness_report_2015_df, world_happiness_report_2016_df, world_happiness_report_2017_df,world_happiness_report_2018_df, world_happiness_report_2019_df]
    World_Happiness_Report_df = pd.concat(frames)

    World_Happiness_Report_df.to_csv ('/home/omar/airflow/dags/data/All.csv', index = False, header=True)
    World_Happiness_Report_df = World_Happiness_Report_df.reset_index()

    Country_Data_250_df = country_data(Country_Data_250_df)

    Life_Expectancy_Data_df = life_expectancy(Life_Expectancy_Data_df)

    Clean_Country_Names(World_Happiness_Report_df)
    Clean_Country_Names(Country_Data_250_df)
    Clean_Country_Names(Life_Expectancy_Data_df)

    label_encoding = preprocessing.LabelEncoder()
    Life_Expectancy_Data_df_encode = Life_Expectancy_Data_df.copy()
    Life_Expectancy_Data_df_encode['Status'] = label_encoding.fit_transform(Life_Expectancy_Data_df_encode['Status'])

    Country_Data_250_df = add_landlocked(Country_Data_250_df)

    Life_Expectancy_Data_df = adding_gdp_cap(gdp_per_capita, Life_Expectancy_Data_df)

    Life_Expectancy_Data_df_Reduced = Life_Expectancy_Data_df_encode.groupby('Country/Region').mean()
    Life_Expectancy_Data_df_Reduced = Life_Expectancy_Data_df_Reduced.reset_index()

    World_Happiness_Report_df_Reduced = World_Happiness_Report_df.copy()
    World_Happiness_Report_df_Reduced = World_Happiness_Report_df_Reduced.groupby('Country/Region').mean()
    World_Happiness_Report_df_Reduced = World_Happiness_Report_df_Reduced.reset_index()
    World_Happiness_Report_df_Reduced.drop(['Year'], axis = 1,inplace = True)

    First_Merge = pd.merge(World_Happiness_Report_df_Reduced,Life_Expectancy_Data_df_Reduced,how = 'inner', on = 'Country/Region')
    Merged_df = pd.merge(First_Merge,Country_Data_250_df,how = 'inner',on = 'Country/Region')

    Merged_df['GDP_per_Country'] = Merged_df["Population"]+Merged_df['GDP']    

    Common_Countries = [World_Happiness_Report_df["Country/Region"], Country_Data_250_df["Country/Region"], Life_Expectancy_Data_df["Country/Region"]]

    #Common_Countries = reduce(np.intersect1d, [World_Happiness_Report_df["Country/Region"], Country_Data_250_df["Country/Region"], Life_Expectancy_Data_df["Country/Region"]])

    removed_countries1 = World_Happiness_Report_df.loc[~World_Happiness_Report_df['Country/Region'].isin(Common_Countries)]['Country/Region'].tolist()
    removed_countries2 = Country_Data_250_df.loc[~Country_Data_250_df['Country/Region'].isin(Common_Countries)]['Country/Region'].tolist()
    removed_countries3 = Life_Expectancy_Data_df.loc[~Life_Expectancy_Data_df['Country/Region'].isin(Common_Countries)]['Country/Region'].tolist()
    removed_countries = removed_countries1 + removed_countries2 + removed_countries3
    unique_removed_countries = unique(removed_countries)
    np.sort(unique_removed_countries)
    Merged_df.drop(['index', 'Happiness_Rank', 'Dystopia_Residual', 'Corruption', 'Year','BMI', 'Real_Growth_Rating_Percentage', 'Schooling', 'Population', 'GDP', 'Generosity', 'Percentage_Expenditure', 'Total_Expenditure', 'Inflation_Percentage', 'Infant_Deaths', 'Adult_Mortality'], axis=1, inplace=True)
    
    return Merged_df
#######################################################################################

######################################## BOXPLOTS #####################################
def show_boxpolt_before_normalization(**context):
    Merged_df = context['task_instance'].xcom_pull(task_ids='load_main_dfs')
    fig, axes = plt.subplots(2,2)
    sns.boxplot(x=Merged_df['Happiness_Score'], ax=axes[0,0])
    sns.boxplot(x=Merged_df['Social_Support'], ax=axes[0,1])
    sns.boxplot(x=Merged_df['Health'], ax=axes[1,0])
    sns.boxplot(x=Merged_df['GDP_Rating'], ax=axes[1,1])
    fig.tight_layout()
    fig.savefig("/home/omar/airflow/dags/data/Boxplot_before_Normalization/Boxplot_for_Happiness_Score_Social_Support_Health_GDP_Rating Before Normalization")

    fig, axes = plt.subplots(2,2)
    sns.boxplot(x=Merged_df['Life_Expectancy'], ax=axes[0,0])
    sns.boxplot(x=Merged_df['GDP_per_Country'], ax=axes[0,1])
    sns.boxplot(x=Merged_df['Unemployment_Percentage'], ax=axes[1,0])
    sns.boxplot(x=Merged_df['Alcohol'], ax=axes[1,1])
    fig.tight_layout()
    fig.savefig("/home/omar/airflow/dags/data/Boxplot_before_Normalization/Boxplot_for_Life_Expectancy_GDP_per_Country_Unemployment_Percentage_GDP_Alcohol Before Normalization")



    fig_dims = (19,15 )
    fig, axes = plt.subplots(2,2)
    sns.boxplot(x=Merged_df['Income_Composition_of_Resources'], ax=axes[0,0])
    sns.boxplot(x=Merged_df['Freedom'], ax=axes[0,1])
    sns.boxplot(x=Merged_df['Vaccination'], ax=axes[1,0])
    sns.boxplot(x=Merged_df['Literacy_Rate_Percentage'], ax=axes[1,1])
    fig.tight_layout()
    fig.savefig("/home/omar/airflow/dags/data/Boxplot_before_Normalization/Boxplot_for_Income_Composition_of_Resources_Freedom_Vaccination_GDP_Literacy_Rate_Percentage Before Normalization")


def normalization_scaling(**context):
    Merged_df = context['task_instance'].xcom_pull(task_ids='load_main_dfs')
    Merged_Tidy_df = normalise_and_scale(Merged_df, ['Social_Support', 'Vaccination', 'Literacy_Rate_Percentage', 'Unemployment_Percentage'])
    Merged_Tidy_df = normalise(Merged_Tidy_df, ['Happiness_Score', 'Health', 'Life_Expectancy', 'Alcohol', 'Income_Composition_of_Resources', 'GDP_per_Country', 'Freedom'])
    Merged_Tidy_df.to_csv('/home/omar/airflow/dags/data/merged_tidy_data.csv', index = False, header=True)
    return Merged_Tidy_df


def show_boxpolt_after_normalization(**context):
    Merged_Tidy_df = context['task_instance'].xcom_pull(task_ids='normalization_scaling')
    fig, axes = plt.subplots(2,2)
    sns.boxplot(x=Merged_Tidy_df['Happiness_Score'], ax=axes[0,0])
    sns.boxplot(x=Merged_Tidy_df['Social_Support'], ax=axes[0,1])
    sns.boxplot(x=Merged_Tidy_df['Health'], ax=axes[1,0])
    sns.boxplot(x=Merged_Tidy_df['GDP_Rating'], ax=axes[1,1])
    fig.tight_layout()
    fig.savefig("/home/omar/airflow/dags/data/Boxplot_after_Normalization/dataBoxplot_for_Happiness_Score_Social_Support_Health_GDP_Rating After Normalization")

    fig, axes = plt.subplots(2,2)
    sns.boxplot(x=Merged_Tidy_df['Life_Expectancy'], ax=axes[0,0])
    sns.boxplot(x=Merged_Tidy_df['GDP_per_Country'], ax=axes[0,1])
    sns.boxplot(x=Merged_Tidy_df['Unemployment_Percentage'], ax=axes[1,0])
    sns.boxplot(x=Merged_Tidy_df['Alcohol'], ax=axes[1,1])
    fig.tight_layout()
    fig.savefig("/home/omar/airflow/dags/data/Boxplot_after_Normalization/dataBoxplot_for_Life_Expectancy_GDP_per_Country_Unemployment_Percentage_GDP_Alcohol After Normalization")

    fig, axes = plt.subplots(2,2)
    sns.boxplot(x=Merged_Tidy_df['Income_Composition_of_Resources'], ax=axes[0,0])
    sns.boxplot(x=Merged_Tidy_df['Freedom'], ax=axes[0,1])
    sns.boxplot(x=Merged_Tidy_df['Vaccination'], ax=axes[1,0])
    sns.boxplot(x=Merged_Tidy_df['Literacy_Rate_Percentage'], ax=axes[1,1])
    fig.tight_layout()
    fig.savefig("/home/omar/airflow/dags/data/Boxplot_after_Normalization/Boxplot_for_Income_Composition_of_Resources_Freedom_Vaccination_GDP_Literacy_Rate_Percentage After Normalization")

def locked_countries(**context):
    Merged_Tidy_df = context['task_instance'].xcom_pull(task_ids='normalization_scaling')
    landlocked = pd.DataFrame()
    not_landlocked = pd.DataFrame()
    for i in Merged_Tidy_df.index:
        if(Merged_Tidy_df['Landlocked'][i] == 1):
            landlocked = landlocked.append(Merged_Tidy_df.loc[i])
        else:
            not_landlocked = not_landlocked.append(Merged_Tidy_df.loc[i])

    not_landlocked.to_csv('/home/omar/airflow/dags/data/not_landlocked.csv', index = False, header=True)
    landlocked.to_csv('/home/omar/airflow/dags/data/landlocked.csv', index = False, header=True)

    data = [['LandLocked', landlocked['GDP_per_Country'].mean()], ['Not_LandLocked', not_landlocked['GDP_per_Country'].mean()]]
    df_GDP_mean_country = pd.DataFrame(data, columns=['Category', 'Mean'])
    df_GDP_mean_country.to_csv('/home/omar/airflow/dags/data/df_GDP_mean_country.csv', index = False, header=True)

    data1 = [['LandLocked', landlocked['Income_Composition_of_Resources'].mean( )], ['Not_LandLocked', not_landlocked['Income_Composition_of_Resources'].mean()]]
    Income_Composition_Of_Resources = pd.DataFrame( data1, columns=['Category', 'Mean'])
    Income_Composition_Of_Resources.to_csv('/home/omar/airflow/dags/data/Income_Composition_Of_Resources.csv', index = False, header=True)

    return not_landlocked, landlocked

def insightsL(**context):
    not_landlocked, landlocked = context['task_instance'].xcom_pull(task_ids='locked_countries')

#ALCOHOL
    fig_dims = (20, 5)
    fig, ax = plt.subplots(1, 2)
    landlocked_graph = sns.distplot(x=landlocked["Alcohol"], ax=ax[0])
    not_landlocked_graph = sns.distplot(x=not_landlocked["Alcohol"], ax=ax[1])
    fig.savefig("/home/omar/airflow/dags/data/insights/Insight on Alcohol")
    #landlocked_graph.set(xlim=(0, 20))
    # landlocked_graph.set(ylim=(0,0.2))
    #not_landlocked_graph.set(xlim=(0, 20))
    # not_landlocked_graph.set(ylim=(0,0.2))
    landlocked_graph.set_title('Landlocked')
    not_landlocked_graph.set_title('Not Landlocked')

#FREEDOM
    fig_dims = (20, 5)
    fig, ax = plt.subplots(1, 2)
    landlocked_graph = sns.distplot(x=landlocked["Freedom"], ax=ax[0])
    not_landlocked_graph = sns.distplot(x=not_landlocked["Freedom"], ax=ax[1])
    fig.savefig("/home/omar/airflow/dags/data/insights/Insight on Freedom")
    #landlocked_graph.set(xlim=(0, 20))
    # landlocked_graph.set(ylim=(0,0.2))
    #not_landlocked_graph.set(xlim=(0, 20))
    # not_landlocked_graph.set(ylim=(0,0.2))
    landlocked_graph.set_title('Landlocked')
    not_landlocked_graph.set_title('Not Landlocked')

#VACCINATION
    fig_dims = (20, 5)
    fig, ax = plt.subplots(1, 2)
    landlocked_graph = sns.distplot(x=landlocked["Vaccination"], ax=ax[0])
    not_landlocked_graph = sns.distplot(x=not_landlocked["Vaccination"], ax=ax[1])
    landlocked_graph.set(xlim=(0, 1))
    landlocked_graph.set(ylim=(0, 2))
    not_landlocked_graph.set(xlim=(0, 1))
    not_landlocked_graph.set(ylim=(0, 2))
    fig.savefig("/home/omar/airflow/dags/data/insights/Insight on Vaccination")
    landlocked_graph.set_title('Landlocked')
    not_landlocked_graph.set_title('Not Landlocked')

#GPD PER COUNTRY
    fig_dims = (20, 5)
    fig, ax = plt.subplots(1, 2)
    landlocked_graph = sns.distplot(x=landlocked["GDP_per_Country"], ax=ax[0])
    not_landlocked_graph = sns.distplot(
    x=not_landlocked["GDP_per_Country"], ax=ax[1])
    landlocked_graph.set(xlim=(0, 0.3))
    landlocked_graph.set(ylim=(0, 80))
    not_landlocked_graph.set(xlim=(0, 0.3))
    not_landlocked_graph.set(ylim=(0, 80))
    fig.savefig("/home/omar/airflow/dags/data/insights/Insight on GDP per Country Density")
    landlocked_graph.set_title('Landlocked')
    not_landlocked_graph.set_title('Not Landlocked')

    fig_dims = (20, 5)

    # fig, ax = plt.subplots(1,2)
    data = [['LandLocked', landlocked['GDP_per_Country'].mean()], ['Not_LandLocked', not_landlocked['GDP_per_Country'].mean()]]
    df_GDP_mean_country = pd.DataFrame(data, columns=['Category', 'Mean'])
    bp_tips = sns.barplot(x='Category', y='Mean', data=df_GDP_mean_country, estimator=np.median)
    bp_tips.set(xlabel="Status", ylabel="GDP_per_Country")
    fig = bp_tips.get_figure()
    fig.savefig("/home/omar/airflow/dags/data/insights/Insight on GDP per Country Mean")
    df_GDP_mean_country
    landlocked_graph.set_title('Landlocked')
    not_landlocked_graph.set_title('Not Landlocked')
    df_GDP_mean_country.to_csv ('/home/omar/airflow/dags/data/df_GDP_mean_country.csv', index = False, header=True)

#HEALTH AND SOCIAL SUPPORT
    fig_dims = (20, 5)
    #fig, ax = plt.subplots(figsize=fig_dims)
    # ,hue = 'Landlocked'

    #SeabornBARCHART = sns.regplot(x = "Happiness Score", y = "Health", ax=ax, data=landlocked)
    SeabornBARCHART = sns.lmplot(x="Social_Support", y="Health", hue="Status", data=landlocked)
    SeabornBARCHART.savefig("/home/omar/airflow/dags/data/insights/Insight on Social_Support vs Health landlocked")
    #SeabornBARCHART.set(xlim=(0, 2))
    #SeabornBARCHART.set(ylim=(0, 1))
    #SeabornBARCHART.set_xticklabels(df_final_normalized['Happiness Score'].values, rotation=90,fontdict={'size':8})#style the x ticks

    #fig_dims = (19,5 )
    #fig, ax = plt.subplots(figsize=fig_dims)
    # ,hue = 'Landlocked'

    #SeabornBARCHART = sns.regplot(x = "Happiness Score", y = "Health", ax=ax, data=landlocked)
    SeabornBARCHART = sns.lmplot(x="Social_Support", y="Health", hue="Status", data=not_landlocked)
    SeabornBARCHART.savefig("/home/omar/airflow/dags/data/insights/Insight on Social_Support vs Health NOTlandlocked")
    #SeabornBARCHART.set(xlim=(0, 2))
    #SeabornBARCHART.set(ylim=(0, 1))
    #SeabornBARCHART.set_xticklabels(df_final_normalized['Happiness Score'].values, rotation=90,fontdict={'size':8})#style the x ticks

#LANDLOCKED
    fig_dims = (20, 5)

    # fig, ax = plt.subplots(1,2)
    data = [['LandLocked', landlocked['Income_Composition_of_Resources'].mean( )], ['Not_LandLocked', not_landlocked['Income_Composition_of_Resources'].mean()]]
    Income_Composition_Of_Resources = pd.DataFrame( data, columns=['Category', 'Mean'])
    bp_tips = sns.barplot(x='Category', y='Mean', data=Income_Composition_Of_Resources, estimator=np.median)
    bp_tips.set(xlabel="Status", ylabel="Income_Composition_of_Resources")
    fig = bp_tips.get_figure()
    fig.savefig("/home/omar/airflow/dags/data/insights/Insight on Income_Composition_of_Resources Mean.png")
    Income_Composition_Of_Resources.to_csv ('/home/omar/airflow/dags/data/Income_Composition_Of_Resources.csv', index = False, header=True)

#HAPPINESS SCORE AND LANDLOCK
    fig_dims = (20, 5)
    fig, ax = plt.subplots(1, 2)
    landlocked_graph = sns.distplot(x=landlocked["Happiness_Score"], ax=ax[0])
    not_landlocked_graph = sns.distplot(x=not_landlocked["Happiness_Score"], ax=ax[1])
    fig.savefig("/home/omar/airflow/dags/data/insights/Insight on Happiness_Score")
    #landlocked_graph.set(xlim=(0, 20))
    landlocked_graph.set(ylim=(0, 2))
    #not_landlocked_graph.set(xlim=(0, 20))
    not_landlocked_graph.set(ylim=(0, 2))
    landlocked_graph.set_title('Landlocked')
    not_landlocked_graph.set_title('Not Landlocked')






def insightsM(**context):
    Merged_Tidy_df = context['task_instance'].xcom_pull(task_ids='normalization_scaling')

#CORRELATION MAP
    fig = plt.figure(figsize=(19, 15))
    plt.matshow(Merged_Tidy_df.corr(), fignum=fig.number)
    plt.xticks(range(Merged_Tidy_df.shape[1]), Merged_Tidy_df.columns, fontsize=14, rotation=90)
    plt.yticks(range(Merged_Tidy_df.shape[1]), Merged_Tidy_df.columns, fontsize=14)
    cb = plt.colorbar(cmap='hot')
    cb.ax.tick_params(labelsize=14)
    fig.savefig("/home/omar/airflow/dags/data/insights/Correlation Map")    

#LANDLOCK VS NON-LANDLOCK    
    sns_plot = sns.histplot(data=Merged_Tidy_df, x="Status", hue="Landlocked", multiple="stack")
    fig = sns_plot.get_figure()
    fig.savefig("/home/omar/airflow/dags/data/insights/count_of_landlocked_based_on_status")
    #INCOME COMPOSITION OF RESOURCES
    #LITERACY
    fig_dims = (20, 5)
    #fig, ax = plt.subplots(figsize=fig_dims)
    # ,hue = 'Landlocked'

    #SeabornBARCHART = sns.regplot(x = "Happiness Score", y = "Health", ax=ax, data=landlocked)
    SeabornBARCHART = sns.lmplot(x="Literacy_Rate_Percentage", y="Income_Composition_of_Resources", hue="Status", data=Merged_Tidy_df, fit_reg=True)
    SeabornBARCHART.savefig("/home/omar/airflow/dags/data/insights/Insight on Literacy_Rate_Percentage vs Income_Composition_of_Resources")
    #SeabornBARCHART.set(xlim=(0, 1))
    #SeabornBARCHART.set(ylim=(0, 1))
    # SeabornBARCHART.set_xticklabels(df_final_normalized['Happiness Score'].values, rotation=90,fontdict={'size':8})#style the x ticks

    #UNEMPLOYMENT RATE
    fig_dims = (20, 5)
    #fig, ax = plt.subplots(figsize=fig_dims)
    # ,hue = 'Landlocked'

    #SeabornBARCHART = sns.regplot(x = "Happiness Score", y = "Health", ax=ax, data=landlocked)
    SeabornBARCHART = sns.lmplot(x='Unemployment_Percentage', y="Income_Composition_of_Resources", hue="Status", data=Merged_Tidy_df, fit_reg=True)
    SeabornBARCHART.savefig("/home/omar/airflow/dags/data/insights/Insight on Unemployment_Percentage vs Income_Composition_of_Resources")
    #SeabornBARCHART.set(xlim=(0, 2))
    #SeabornBARCHART.set(ylim=(0, 1400))
    # SeabornBARCHART.set_xticklabels(df_final_normalized['Happiness Score'].values, rotation=90,fontdict={'size':8})#style the x ticks

    #GDP
    fig_dims = (20, 5)
    #fig, ax = plt.subplots(figsize=fig_dims)
    # ,hue = 'Landlocked'

    #SeabornBARCHART = sns.regplot(x = "Happiness Score", y = "Health", ax=ax, data=landlocked)
    SeabornBARCHART = sns.lmplot(x='GDP_Rating', y="Income_Composition_of_Resources", hue="Status", data=Merged_Tidy_df, fit_reg=True)
    SeabornBARCHART.savefig("/home/omar/airflow/dags/data/insights/Insight on GDP_Rating vs Income_Composition_of_Resources")
    #SeabornBARCHART.set(xlim=(0, 2))
    #SeabornBARCHART.set(ylim=(0, 1400))
    # SeabornBARCHART.set_xticklabels(df_final_normalized['Happiness Score'].values, rotation=90,fontdict={'size':8})#style the x ticks

    #LIFE EXPECTANCY
    fig_dims = (20, 5)
    #fig, ax = plt.subplots(figsize=fig_dims)
    # ,hue = 'Landlocked'

    #SeabornBARCHART = sns.regplot(x = "Happiness Score", y = "Health", ax=ax, data=landlocked)
    SeabornBARCHART = sns.lmplot(x='Income_Composition_of_Resources', y="Life_Expectancy", hue="Status", data=Merged_Tidy_df, fit_reg=True)
    SeabornBARCHART.savefig("/home/omar/airflow/dags/data/insights/Insight on Life_Expectancy vs Income_Composition_of_Resources")
    #SeabornBARCHART.set(xlim=(0, 2))
    #SeabornBARCHART.set(ylim=(0, 1400))
    # SeabornBARCHART.set_xticklabels(df_final_normalized['Happiness Score'].values, rotation=90,fontdict={'size':8})#style the x ticks

#HAPPINESS SCORE
    #fig_dims = (20,5 )
    #fig, ax = plt.subplots(figsize=fig_dims)
    
    SeabornBARCHART = sns.lmplot(x="Income_Composition_of_Resources", y="Happiness_Score", data=Merged_Tidy_df)
    SeabornBARCHART.savefig("/home/omar/airflow/dags/data/insights/Insight on Happiness_Score vs Income_Composition_of_Resources")
    #SeabornBARCHART.set(xlim=(0, 8))
    #SeabornBARCHART.set(ylim=(0, 1))
    # SeabornBARCHART.set_xticklabels(df_final_normalized['Happiness Score'].values, rotation=90,fontdict={'size':8})#style the x ticks





def statusinsight(**context):
    Merged_Tidy_df = context['task_instance'].xcom_pull(task_ids='normalization_scaling')
    not_landlocked, landlocked = context['task_instance'].xcom_pull(task_ids='locked_countries')
    #STATUS AND LANDLOCKED
    fig_dims = (20, 5)
    fig, ax = plt.subplots(figsize=fig_dims)

    g = sns.barplot(x="Country/Region", y="Happiness_Score", data=Merged_Tidy_df, hue='Status')
    g.set_xticklabels(Merged_Tidy_df['Country/Region'].values, rotation=90, fontdict={'size': 8})
    fig.savefig("/home/omar/airflow/dags/data/insights/Happiness_Score across all Countries on all data")
    g.set_title('Status Classification')

    fig_dims = (20, 5)
    fig, ax = plt.subplots(figsize=fig_dims)

    g = sns.barplot(x="Country/Region", y="Happiness_Score", data=landlocked, hue='Status')
    g.set_xticklabels(Merged_Tidy_df['Country/Region'].values, rotation=90, fontdict={'size': 8})
    fig.savefig("/home/omar/airflow/dags/data/insights/Happiness_Score across all Countries on Landlocked")
    g.set_title('Status and landlocked Classification')

    fig_dims = (20, 5)
    fig, ax = plt.subplots(figsize=fig_dims)

    g = sns.barplot(x="Country/Region", y="Happiness_Score", data=not_landlocked, hue='Status')
    g.set_xticklabels(Merged_Tidy_df['Country/Region'].values, rotation=90, fontdict={'size': 8})
    fig.savefig("/home/omar/airflow/dags/data/insights/Happiness_Score across all Countries on NOTLandlocked")
    g.set_title('Status and not landlocked Classification')

t1 = PythonOperator(
    task_id='load_main_dfs',
    provide_context=False,
    python_callable=load_main_dfs,
    dag=dag,
)

t2 = PythonOperator(
    task_id='show_boxpolt_before_normalization',
    provide_context=True,
    python_callable=show_boxpolt_before_normalization,
    dag=dag,
)
t3 = PythonOperator(
    task_id='normalization_scaling',
    provide_context=True,
    python_callable=normalization_scaling,
    dag=dag,
)
t4 = PythonOperator(
    task_id='show_boxpolt_after_normalization',
    provide_context=True,
    python_callable=show_boxpolt_after_normalization,
    dag=dag,
)

t5 = PythonOperator(
    task_id='locked_countries',
    provide_context=True,
    python_callable=locked_countries,
    dag=dag,
)




##################################################################################
# step 5 - define dependencies

t1 >> t2 >> t3 >> t4 >> t5
