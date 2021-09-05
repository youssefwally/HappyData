## DAG:

### ```pipeline-dag```
Fetches and preprocess the happiness score, 250 country and life expectancy data to be fully ready for visualization. In addition, fetches and preprocess new data by webscraping them and adding them to the already given data. 

##Functions 

### ```unique(list):```
	returns a list containing only unique values. 

### ```Clean_Country_Names(list_of_country_names):```
	Changes country names so that countries spelled differently or in their official name are referred to in their more common name. For example, The People’s Republic of China is changed to China. 

### ```isNan(string):```
	returns a boolean value indicating whether or not the string entered is NaN.

### ```Replace_NAN_With_Mean(df, column_name):```
	imputes NaN values with the clolumn’s mean. 

### ```def normalise_and_scale(df, columns):```
	transforms the data in the column using boxcox. However because boxcox only works with positive values and some of the columns have zero values any column containing zero values will have its data incremented by 0.1 then boxcoc applied. Afterwards, min max scaler is applied to scale the data from 0 to 1.

### ```normalise(df, columns):```
	transforms the data in the column using a min max scaler to scale the data from 0 to 1. 

### ```Remove_Percentage_Year(df)```
removes symbols from the dataframe so that we are only left with numeric values.

### ```get_landlocked_countries()```
uses beautiful soup to webscrapce https://en.wikipedia.org/wiki/Landlocked_country to get the landlocked countries.  

###  ```webscraping_landlocked()```
	uses get_landlocked_countries to fetch the landlocked countries, then cleans the country names before returning them. 

### ```add_landlocked(df)```
	adds the landlocked countries to a dataframe

### ```get_urls(string)```
reads from json file in the url:
https://www.macrotrends.net/assets/php/global_metrics/country_search_list.php/string
Then for each country in the json file, it creates a url that will be used to webscrape for the attribute needed.  

### scrape_macrotrends(urls)
takes an array of urls, each country has only one url, and webscrapes theses urls to retrieve data from the macrotrends website. It then appends that data to triple (country name, year, data)
### adding_gdp_cap(gdp_per_capita, df):
Since gdp_per_capita was missing lots of values in the Life_expectancy_df, we decided to web scrape macrotrends to retrieve that data. This function integrates the web scraped data that is given to this function through the gdp_per_capita variable and integrates it with the given df. 
	
### ```def WHR_2015-WHR_2019: ```
These functions take in the world happiness dataframe. They then proceed to clean and tide the data by changing the column names into a more standard format and understandable titles. They also engineer a new column called Dystopia_Residual by adding the happiness score, GDP per capita, social support, Health, freedom to make life choices and Corruption. These procedures are done for all the world happiness dataframe of 2015 till 2019.

### ```def country_data(Country_Data_250_df): ```
This function takes in the dataframe of the 250 country data. It then cleans and tides the data by changing the column names into more appropriate and understandable names. It also drops all the NAN values in the database and replaces them with the mean value of each column.

### ```def  life_expectancy(Life_Expectancy_Data_df): ```
This function takes in the life expectancy dataframe. It then cleans and tides the data by first changing the column names into more appropriate names.  It removes the NAN values in the columns and replaces them with the mean values of each column. It adds a new column called Vaccination that adds the values of Hepatitis_B, Polio and Diphtheria and then drops them all.

### ```def load_data(path): ```
This function is used as a shortcut to use pandas to read in the csv files to a dataframe that can be used,

### ```def load_main_dfs(): ```
This is the main function that is used to call all the other functions in the right order. The order of the functions called is as follows:
Uses the ``` def load_data() ``` function to load all the dataframes required.
Adds a year column to all the world happiness dataframes appropriately for each one from 2015 to 2019
Calls the ```get_inflation()``` function to web scrape the required data and assigns it to a variable ``` inflation```
Does the same with scrape_macrotrends to get the gdp_per_capita data.
Cleans up, tides and engineers to columns to the data frames world happiness 2015-2019
Adds all the world happiness data frames into one large data frame then places a csv file with the acquired data locally.
Cleans, tides and adds columns to 250 Country data frames using the ``` def Clean_Country_Names()``` function.
Does the same for Life expectancy data frame with using the function ```life_expectancy()```
It then unifies the country names in all the obtained data frames; world happiness, Country data and life expectancy; using the Clean_Country_Names()
It then takes a copy of the life expectancy data frame and adds a status column that is label encoded.
It adds a column in the 250 country data that states whether each of the countries is landlocked or not
It adds the obtained gdp per capita from each country to the life expectancy data frame
It groups each country in the life expectancy and gets their mean values.
It does the same for the world happiness data frame.
It merges all the data frames together on the Country/Region column, Merged_df
It engineers a new column, GDP per Country, using the population and GDP columns
It then removes all the unwanted columns from the Merged_df data frame

## Tasks
### ```The function that each task calls```
The first task, load_main_dfs, calls the load_main_dfs() method which in turn handles the loading of dataframes, and their cleaning as explained above.
The second task, show_boxpolt_before_normalization, is used to display the data especially accounting for the outliers.
The third task, normalization_scaling, normalizes the data so that they can be used in the following task.
The fourth task, show_boxpolt_after_normalization, displays the data after normalization. This serves as a comparison between the data previously and the data now to see how well has the data been adujsted.
The fifth and final task, locked_countries, creates two new data frames. One data frame hosts landlocked countries and their information, while the other hosts non-landlocked countries and their information. It then writes them to a csv file before using the data to create two comparisons between both sets of countries. The first insights compares between both sets of countries based on average GDP, while the other insight shows the comparison between them in a metric labeled the Income Composition Of Resources.

#### T1 >> load_main_dfs
#### T2 >> show_boxpolt_before_normalization
#### T3 >> normalization_scaling
#### T4 >> show_boxpolt_after_normalization
#### T5 >> locked_countries


## Dependencies
### ```Sequence of tasks execution```

#### T1 >> T2 >> T3 >> T4 >> T5
