# spark-assignment

Tasks to be performed 

* Read the data, show it and Count the number of records.
* Describe the data with a describe function.
* If there is any duplicate value drop it.
* Use limit function for showcasing a limited number of records.
* If you find the column name is not suitable, change the column name.[optional]
* Select the subset of the columns.
* If there is any null value, fill it with any random value or drop it.
* Filter the data based on different columns or variables and do the best analysis.

For example: We can filter a data frame using multiple
conditions using AND(&), OR(|) and NOT(~) conditions. For
example, we may want to find out all the dif erent

infection_case in Daegu Province with more than 10 confirmed cases.
* Sort the number of confirmed cases. Confirmed column is there in the dataset. Check with descending sort also.
* In case of any wrong data type, cast that data type from integer to string or string to integer.
* Use group by on top of province and city column and agg it with sum of confirmed cases. For example
  df.groupBy(["province","city"]).agg(function.sum("confirmed")

For joins we will need one more file you can use region file.
User different different join methods.for example
cases.join(regions, ['province','city'],how='left')
