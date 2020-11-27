# Movies-ETL

## Objectives
The goals of this challenge are:

- Create an automated ETL pipeline.
- Extract data from multiple sources.
- Clean and transform the data automatically using Pandas and regular expressions.
- Load new data into PostgreSQL.

## Assumptions
- If the same type of files are used (wikipedia_json, kaggle_csv and ratings_csv), you need to consider the following:
	- All the original column names for the data files needs to be named the same way the function states.
	- To clean the data, for example: removing the dollar sign to a number, the regular expression operations used in the function should match the new data to apply successful.
	- All the columns used in the function are in the new data.
	- We are assuming we only need to update the data type for the columns states in the function.
	- When loading the data to SQL, we are assuming the tables exist already, so we are replacing the tables in case they exist.
	- Use try-except blocks to account for unforeseen problems that may arise with new data.

## Observations
- Create a function that takes three arguments, in this case:
	- Wikipedia data
	- Kaggle metadata
	- MovieLens rating data (from Kaggle)
- Try and except blocks can be used to let know the user where exactly the function can have issues like KeyError.  We can try to pass columns like budget, runtime, and just do not include in the final dataframe, but it is important to know there are columns like imbd_id, that is the column the function used to merge the dataframes, that cannot be passed because the imbd_id is used to merge the dataframes, so the function needs to be adjusted.  Notice if budget or runtime for example are not included in the new data, the function needs to be adjusted to ignore those columns.  In the case the imbd_id column is named different in the new file to the name in the function, we can just update the name in the function.
- Before applying the function, first you need to make sure all the columns called inside the function exist in the files, if the name column is different you would need to adjust the function.  The regular expressions used also need to match the format to the new data files to make the proper transformations.
- In case we need to transform a column and this one is not included in the function; we can adjust the function to add the new column if any of the functions define inside the Ext_Trans can be applied.
