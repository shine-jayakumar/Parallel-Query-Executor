# Parallel Query Executor
Runs mysql queries in parallel

## Examples
#### Get result from a SELECT statement
```
payload = [
    {
        "query": "SELECT * FROM city"
    },
    {
        "query": "DELETE FROM city WHERE Name='New York'"
    }
]

# run all queries in parallel
pqexec = ParallelQueryExecutor(DB_HOST, DB_USER, DB_PASS, DB, payload, auto_commit=True)
pqexec.runall()

# print result received from the SELECT query
for task, val in pqexec.tasks.items():
    if val.get('status', None):
        if val['status'] == True and val['query'] == 'SELECT * FROM city':
            print(val['result'])
            break

```


#### Commit only if all the INSERT queries were successful
```
payload = [
    {
    "query": "INSERT INTO city (ID, Name, CountryCode) VALUES (%s, %s, %s)",
    "data": (None, 'New York', 'USA')
    },
    {
    "query": "INSERT INTO city (ID, Name, CountryCode) VALUES (%s, %s, %s)",
    "data": (None, 'Kabul', 'AFG')
    }
]



# don't close connection; don't auto-commit
pqexec = ParallelQueryExecutor(DB_HOST, DB_USER, DB_PASS, DB, payload, auto_commit=False, close_conn=False)
pqexec.runall()


# commit later if all the INSERT statements
# executed successfully

insert_successful = True

for task, val in pqexec.tasks.items():
    if val.get('status', None):
        if val['status'] == False:
            insert_successful = False
            break

# if all INSERT statements were successful, commit
if insert_successful:
    for task, val in pqexec.tasks.items():
        val['connection'].commit()
```
