# TableauDataSource

## Description
The TableauDataSource class is a Python class that provides a simple interface for creating and manipulating Tableau data sources using the Tableau Hyper API. It allows you to create tables, append rows to existing tables, update rows in existing tables, and delete rows from existing tables.

## Example Usage
```python
from tableau_datasource import TableauDataSource

# Create a Tableau data source object
datasource = TableauDataSource("mydatasource.hyper")

# Define the table schema
table_definition = {
    "id": tab_api.SqlType.int(),
    "name": tab_api.SqlType.text(),
    "age": tab_api.SqlType.int()
}

# Create a new table
datasource.create_table("mytable", table_definition)

# Append rows to the table
rows = [
    (1, "John", 30),
    (2, "Jane", 25),
    (3, "Bob", 40)
]
datasource.append_rows("mytable", rows)

# Update rows in the table
update_query = "UPDATE mytable SET age=35 WHERE name='John'"
datasource.update_rows("mytable", update_query)

# Delete rows from the table
delete_query = "DELETE FROM mytable WHERE age > 30"
datasource.delete_rows("mytable", delete_query)
```

# TableauScheduler
## Description
The TableauScheduler class is a Python class that provides a simple interface for scheduling and managing tasks on Tableau Server using the Tableau Server REST API. It allows you to schedule jobs that run scripts with specified arguments at specified frequencies, run scheduled jobs immediately, modify scheduled jobs, and delete scheduled jobs.

Example Usage
```python
from tableau_scheduler import TableauScheduler

# Create a Tableau scheduler object
scheduler = TableauScheduler("https://mytableauserver.com", "my_personal_access_token")

# Schedule a job
scheduler.schedule_job("myjob", 60, "2023-02-23T12:00:00Z", "/path/to/script.py", ["arg1", "arg2"])

# Run a scheduled job immediately
scheduler.run_job("job_id")

# Modify a scheduled job
job_properties = {
    "name": "new_name",
    "frequency": {"intervalInMinutes": 120},
    "startTime": "2023-02-24T12:00:00Z"
}
scheduler.modify_job("job_id", job_properties)

# Delete a scheduled job
scheduler.delete_job("job_id")
```
# Testing Methodologies
Both the TableauDataSource and TableauScheduler classes have been extensively tested using the unittest framework. The tests cover all of the methods in both classes and ensure that they are functioning correctly. Mocking is used extensively to simulate Tableau Server and Tableau Hyper API responses, allowing the tests to be run in a controlled environment.

To run the tests for TableauDataSource and TableauScheduler, navigate to the directory containing the tests and run the following command:

```bash
python -m unittest discover
```
This will discover and run all of the tests in the directory.