# DLG-Data-Engineer-Test
DLG Data Engineer Test

Assumptions:
Day temperature: I have used the average temperature between 8am to 4pm to calculate the day temperature since it is a winter month.
Hottest day: The day with the highest average day temperature is considered as the hottest day.
raw group: I assume you meant row group in parquet. I have set the row group value to 30 which is approximately the number of days in the month.

Transformation and data cleansing:
1.	If the ScreenTemperature is -99 reset it to Nan as I believe that the temperature was not captured for that period
2.	If there is a missing temperature between 2 times period, I have populated it by getting the average of previous temperature and next temperature.
Eg: 8:00am is 10 degree, 9:00am is Nan, 11:00am is 12degree
I have transformed it t0 8:00am is 10 degree, 9:00am is 11degree, 11:00am is 12degree
3.	When most of the temperature values are missing in a group then the average is the sum of values present divided by 9.
Eg: 1st of March for site code 3227 has temperature value of 13.6 at 8:00am but the temperature is not present for rest of the day till 4:00pm. Although it is the highest temperature in 2 months the average drops as we are not sure of rest of the temperature for that day.
4.	Removed sitecode from sitename as it is redundant.
5.	There were few country values missing, I populated it based on the region by getting the values from the group.
6.	Changed the observation time to time format.
7.	Removed time from observation date.

I have created 2-dimension tables one for location and other for site. The id’s for these tables are hash keys created from cardinality columns.
There will be a fact table created for each month and I would be a partitioned table on a real-world scenario.
I have created a module for fact_dimension helper which will insert the foreign key to the fact table.
Fact_aggregate is the aggregated table that can be queried to answer the questions. This will have 1 row for the combination of observation date and sitecode.  The metrics are average temperature column ‘avg_temp’ and a ‘temperature’ column which is a dictionary containing all the temperature from 8am to 4pm.
Testing:
Ideally, I would write a unit test for each function defined. However, due to time constraint I have just written unit test for one function.
Output:
All the parquet file outputs are in the output_data folder.
Result:
Based on my assumptions on the data the hottest day in the 2 month period is 21st of Feb 2016. The output is in a csv in output folder.
