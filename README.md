# Movies-ETL
## Assumptions
- If the same type of files are used (wikipedia_json, kaggle_csv and ratings_csv), you need to consider the following:
	- All the original column names for the data files needs to be named the same way the function states.
	- To clean the data, for example: removing the dollar sign to a number, the regular expression operations used in the function should match the new data to apply succesfull.
	- All the columns used in the funtion are in the new data.
	- We are assuming we only need to update the data type for the columns states in the function.
	- When loading the data to SQL, we are assuming the tables exist already, so we are replacing the tables in case they exist.

## Observations
- Try and except blocks can be used to let know the user where exactly the function can have issues like KeyError.  We can try to pass columns like budget, runtime, and just do not include in the final dataframe, but it is important to know there are columns like imbd_id, that is the column the function used to merge the dataframes, that cannot be passed because the imbd_id is used to merge the dataframes, so the function needs to be adjusted.
- Notice the dataframes can be only printed if the print statement is called inside the function.  When I try to print the dataframes created after running the function outside the funtion Extr_Trans, generates an error the dataframes do not exists.  I think this is the reason we need to put everything inside the function including the load of the dataframes to SQL. 
- Before applying the function, first you need to make sure all the columns called inside the function exist in the files, if the name column are different you would need to adjust the function.  The regular expressions used also need to match the format to the new data files.
