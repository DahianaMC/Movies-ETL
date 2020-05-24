# Movies-ETL
## Name of the funtion created to take the 3 data files: Ext_Trans(wiki_json, kaggle_csv, ratings_csv)
## Assumptions
- If the same type of files are used (wikipedia_json, kaggle_csv and ratings_csv), you need to consider the following:
	- All the original column names for the data files needs to be named the same way the function states.
	- To clean the data, for example: removing the dollar sign to a number, the regular expression operations used in the function should match the new data to apply succesfull.
	- All the columns used in the funtion are in the new data.
	- We are assuming we only need to update the data type for the columns states in the function.
	- When loading the data to SQL, we are assuming the tables exist already, so we are replacing the tables in case they exist.


## Observations
- Try and except blocks can be used to let know the user where exactly the function can have issues like KeyError.  We can try to pass columns like budget, runtime, and just do not include in the final dataframe, but it is important to know there are columns like imbd_id, that is the column the function used to merge the dataframes, that cannot be passed because the imbd_id is used to merge the dataframes, so the function needs to be adjusted.  Notice if budget or runtime for example are not included in the new data, the function need to be adjusted to ignore those columns.
- Notice the dataframes can be only printed if the print statement is called inside the function.  Print the dataframes created inside the function after running the function Ext_Trans, generates an error the dataframes do not exists.  Looks like this is an important reason why the challenge is asking to have the 3 arguments (3 data files) inside the new function Ext_Trans.
- Before applying the function, first you need to make sure all the columns called inside the function exist in the files, if the name column are different you would need to adjust the function.  The regular expressions used also need to match the format to the new data files to make the proper transformations.
- In case we need to transform a column and this one is not included in the function, we can adjust the function to add the new column if any of the functions define inside the Ext_Trans can be applied.
