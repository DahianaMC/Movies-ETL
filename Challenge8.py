#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import Dependencies
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from config import db_password
import time
import re


# In[2]:


# Movies file directory
file_dir = 'C:/Users/dahia/Desktop/Analysis_Projects/Movies-ETL/Resources/'


# In[3]:


# Read data files
# Load JSON into a List of Dictionaries
with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
# Extract the Kaggle Data
kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
ratings = pd.read_csv(f'{file_dir}ratings.csv')


# In[61]:


# Create a function that takes in three arguments

def Ext_Trans(wiki_movies_raw, kaggle_metadata, ratings):
    
    # Wikipedia Data JSON format
    
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]


    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune-Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    # Clean Movies
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)  
    
    print("wiki_movies_df has been created, starting cleaning wikipidea data")
    
    # Using regular expressions "regex" to extract the IMDB ID
    # Assuming the imdb colums is always named like 'imdb_link'.  In case the name is different print
    # "Check column name for imbd and update the function"
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    except KeyError:
        print('Column imbd has a different name, update the name in the funtion')
        return
                

    # Drop any duplicates of IMDb IDs by using the drop_duplicates() method
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    
    # Select the columns left after removing the columns with null values 
    # from the wiki_movies_df using pandas.
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    # Drop NaN values in box office.
    box_office = wiki_movies_df['Box office'].dropna()
    
    # Concatenates list items to one string using join()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Parse the Box Office Data
    # Use regular expressions to convert the box office data as a number
    # There are 3 main fomrs the box office data is written in: “$123.4 million” (or billion), and “$123,456,789.” 
    # First match the form "$123.4 million/billion" 
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    # Second match the form "123,456,789"
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+'
    
    # Some values are given as a range.
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # Function to turn the extracted values into a numeric value
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan    
    
    
    # Extract the values from box_office using str.extract.
    # Then we'll apply parse_dollars to the first column in the DataFrame 
    # returned by str.extract
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    # We no longer need the Box Office column, just drop it
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    print("box_office has been created and box office has been dropped")
    
    # Parsing the budget data, create a budget variable.
    budget = wiki_movies_df['Budget'].dropna()
    # Convert any list to a string
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')
          
    # Parse the budget values
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # Drop the original Budget column
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
   
    print("budget has been created and Budget has been dropped")

    # Parse Release Date
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # Regular expressions for release_date
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    # Applying the to_datetime pandas method to release_date instead of
    # using the parse function
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    # Drop Release date (old column) from the dataset
    wiki_movies_df.drop('Release date', axis=1, inplace=True)
    
    print("release_date has been created and Release date has been dropped")
    
    # Parse Running Time
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    # Convert string to numeric values
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    # Apply a function that will convert the hour capture groups and minute 
    # Capture groups to minutes if the pure minutes capture group is zero
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    # Drop Running time (old column) from the dataset
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    print("running_time has been created and Running time has been dropped")
    
    # Kaggle data csv file
    # Drop adult movies.  Keep rows where the adult column is false, then drop 
    # the adult column.
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    # Convert data types for video from object to Boolean
    kaggle_metadata['video'] == 'True'
    # Assign back the column just created to the dataframe
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    # Convert budget, ID and popularity to numeric using to_numeric() method from Pandas
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    # Conver release_date to datetime with Pandas
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    print("Kaggle data has been cleaned")
    
    # Ratings Data csv file
    # Specify in to_datetime() that the origin is 'unix' and the time unit is seconds.
    # Assign the timestamp column
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    
    print("ratings data has been cleaned")
    
    # Merge Wikipedia and Kaggle Metadata using movie ID with an inner join,
    # only want the movies that are in both tables.
    # use the suffixes parameter to make it easier to identify which table each column came from.
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
 
    # After comparing both columns merged from Wikipedia and Kaggle files, the columns selected are the following:
    # 1. Drop the title_wiki, release_date_wiki, Language, and Production company(s) columns.
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    # 2. Make a function that fills in missing data for a column pair and then 
    # drops the redundant column.
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)

     # 3. Run the function for the three column pairs that need to be filled 
    # in zeros.
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')   
    
    # Check there aren’t any columns with only one value, since that doesn’t 
    # really provide any information. Don’t forget, we need to convert lists 
    # to tuples for value_counts() to work.
    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
        if num_values == 1:
            print('Column with only one value:', col)
            
    # Video only has one value.  This column is not needed.
    movies_df['video'].value_counts(dropna=False)

    # Reorder columns:
    # Consider the columns roughly in groups, like this:
    # 1. Identifying information (IDs, titles, URLs, etc.)
    # 2. Quantitative facts (runtime, budget, revenue, etc.)
    # 3. Qualitative facts (genres, languages, country, etc.)
    # 4. Business data (production companies, distributors, etc.)
    # 5. People (producers, director, cast, writers, etc.)
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]

    # Rename the columns.
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)
    
    print("Wikipedia and kaggle data has been merged and merged df has been cleaned")
    
    # Transform and Merge Rating Data
    # Rename the “userId” column to “count.”  We can use either userId or timestamp
    # Pivot this data so that movieId is the index, the columns will be all 
    # the rating values, and the rows will be the counts for each rating value
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                     .rename({'userId':'count'}, axis=1)                     .pivot(index='movieId',columns='rating', values='count')
    
    # Rename the columns so they’re easier to understand.  Add rating_ to each 
    # column using a list comprehension
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    
    # Merge the rating counts into movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    # Fill missing values with zero
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

    print("rating data has been transformed and merged")
    
    print("Verifying the dataframes were succesfully created and merged")
    print("movies_df")
    print(movies_df.columns)
    print("movies_with_ratings_df")
    print(movies_with_ratings_df.columns)
    

    # Connect Pandas and SQL.  Load Data to SQL
    # Create a Database in SQL and import sqlalchemy import create_engine at the top cell
    # Create the Database Engine
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    
    
    # Create the database engine
    engine = create_engine(db_string)
    
    # Import the Movie Data
    movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    
    # Import the Ratings Data

    print("Start loading movie data")
    # create a variable for the number of rows imported
    rows_imported = 0

    # get the start_time from time.time()
    start_time = time.time()
    
    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):

        # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')

        data.to_sql(name='ratings', con=engine, if_exists='replace')

        # increment the number of rows imported by the size of 'data'
        rows_imported += len(data)

        # add elapsed time to final print out
        # print that the rows have finished importing
        print(f'Done. {time.time() - start_time} total seconds elapsed')


# In[63]:


# Apply Ext_Trans funtion
wiki_json = wiki_movies_raw
kaggle_csv = kaggle_metadata
ratings_csv = ratings

Ext_Trans(wiki_json, kaggle_csv, ratings_csv)

print("Data transformed has finished and loaded to SQL")

