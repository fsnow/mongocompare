# mongocompare
Compares two MongoDB collections. The [dbHash command](https://docs.mongodb.com/manual/reference/command/dbHash/) is the most accurate comparison of two collections, but is not feasible in certain situations because it can take a long time to run. A common use case is after a database migration where the collection comparison needs to run in a short maintenance window as application connections are switched to the new cluster. (Hence the parameter naming with "source" and "target")

mongocompare makes the following checks:

* document counts
* indexes match
* document content matches for a random sample of documents



|   Flag     |  Argument Type  |  Description    |  Environment Variable    |
| ---------- | --------------- | --------------- | ------------------------ |    
| --randomSampleSize | int | random sample size used for content comparison (default 100) | (none) |
| --sourceURI | string | source connection string | SOURCE_URI |
| --sourceUsername | string | source username | SOURCE_USERNAME |
| --sourcePassword | string | source password | SOURCE_PASSWORD |
| --sourceDatabase | string | source database | SOURCE_DATABASE |
| --sourceCollName | string | source collection | SOURCE_COLLECTION |
| --targetURI | string | target connection string | TARGET_URI |
| --targetUsername | string | target username | TARGET_USERNAME |
| --targetPassword | string | target password | TARGET_PASSWORD |
| --targetDatabase | string | target database | TARGET_DATABASE |
| --targetCollName | string | target collection | TARGET_COLLECTION |


Example with all command-line args:

```
go run mongocompare.go --sourceURI "mongodb+srv://cluster1.orgcode.mongodb.net/db" --sourceDatabase=sample_airbnb --sourceCollName listingsAndReviews --sourceUsername myuser --sourcePassword mypassword --targetURI "mongodb+srv://cluster2.orgcode.mongodb.net/db" --targetDatabase sample_airbnb2 --targetCollName listingsAndReviews2 --targetUsername myuser --targetPassword mypassword --randomSampleSize 50
```

Example where the usernames and passwords are stored as environment variables:

```
export SOURCE_USERNAME=myuser
export SOURCE_PASSWORD=mypassword
export TARGET_USERNAME=myuser2
export TARGET_PASSWORD=mypassword2

go run mongocompare.go --sourceURI "mongodb+srv://cluster1.orgcode.mongodb.net/db" --sourceDatabase=sample_airbnb --sourceCollName listingsAndReviews --targetURI "mongodb+srv://cluster2.orgcode.mongodb.net/db" --targetDatabase sample_airbnb2 --targetCollName listingsAndReviews2 --randomSampleSize 50
```
