1. Brief description of your approach for each problem

Problem 1:
I connected Spark to read the NYC Taxi data and ran basic transformations to count trips and check a sample of the data. I focused on keeping the code clean and verifying that Spark was running correctly on the cluster. 

Problem 2:
Then for the second, I worked with large log files from S3. I used PySpark to extract timestamps and IDs from each log line, then grouped them by application and cluster to find when each one started and ended. I then saved the results and made a few simple plots to show the timelines and summaries.

2. Key findings and insights from the data

Problem 1:
Most of the log entries were informational, showing that the system ran normally with very few issues. INFO messages made up about 99.9% of all logs, while WARN and ERROR messages were extremely rare. Overall, the data suggests the system was stable during this period.

Problem 2:




3. Performance observations (execution time, optimizations)

Problem 1:
The Spark job ran efficiently overall, with most of the time spent reading large log files from S3. Once the data was loaded, the log counting and sampling finished quickly because Spark reused cached data. The main improvement would be to reduce shuffle operations and adjust partition settings for faster I/O.


Problem 2:
Took a long time to run, and I had lots of errors on this problem.



4. Screenshots of Spark Web UI showing job execution





5. Explanation of the visualizations generated in Problem 2