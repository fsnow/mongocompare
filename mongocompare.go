package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/google/go-cmp/cmp"
	flag "github.com/spf13/pflag"
)

func main() {
	var sourceURI, sourceUsername, sourcePassword, sourceDatabase, sourceCollName string
	stringFlag(&sourceURI, "sourceURI", "source connection string", "SOURCE_URI")
	stringFlag(&sourceUsername, "sourceUsername", "source username", "SOURCE_USERNAME")
	stringFlag(&sourcePassword, "sourcePassword", "source password", "SOURCE_PASSWORD")
	stringFlag(&sourceDatabase, "sourceDatabase", "source database", "SOURCE_DATABASE")
	stringFlag(&sourceCollName, "sourceCollName", "source collection", "SOURCE_COLLECTION")

	var targetURI, targetUsername, targetPassword, targetDatabase, targetCollName string
	stringFlag(&targetURI, "targetURI", "target connection string", "TARGET_URI")
	stringFlag(&targetUsername, "targetUsername", "target username", "TARGET_USERNAME")
	stringFlag(&targetPassword, "targetPassword", "target password", "TARGET_PASSWORD")
	stringFlag(&targetDatabase, "targetDatabase", "target database", "TARGET_DATABASE")
	stringFlag(&targetCollName, "targetCollName", "target collection", "TARGET_COLLECTION")

	var randomSampleSize int
	flag.IntVar(&randomSampleSize, "randomSampleSize", 100, "random sample size used for content comparison")

	flag.Parse()

	// TODO: set the values from env vars here. As it is, they are being set as
	// default values. The only problem with that is that the values get printed in --help.

	sourceCredential := options.Credential{
		Username: sourceUsername,
		Password: sourcePassword,
	}
	sourceClientOps := options.Client().ApplyURI(sourceURI).SetAuth(sourceCredential).SetAppName(mongoAppName())
	sourceClient, err := mongo.Connect(context.TODO(), sourceClientOps)
	if err != nil {
		log.Fatal(err)
	}
	sourceDB := sourceClient.Database(sourceDatabase)
	sourceCollection := sourceDB.Collection(sourceCollName)

	var targetCredential = options.Credential{
		Username: targetUsername,
		Password: targetPassword,
	}
	targetClientOps := options.Client().ApplyURI(targetURI).SetAuth(targetCredential).SetAppName(mongoAppName())

	targetClient, err := mongo.Connect(context.TODO(), targetClientOps)
	if err != nil {
		log.Fatal(err)
	}

	targetDB := targetClient.Database(targetDatabase)
	targetCollection := targetDB.Collection(targetCollName)

	fmt.Println("")
	checkCountsResult := checkCounts(sourceCollection, targetCollection)
	indexCompareResult := compareIndexes(sourceCollection, targetCollection)
	sampleContentResult := compareSampleContent(sourceCollection, targetCollection, randomSampleSize)
	if checkCountsResult && indexCompareResult && sampleContentResult {
		fmt.Println("Passed all validation checks")
	} else {
		fmt.Println("Some validation checks failed. See above.")
	}
	fmt.Println("")
}

func stringFlag(pstr *string, flagName string, helpMessage string, envVar string) {
	envVal := os.Getenv(envVar)
	defVal := ""
	if len(envVal) > 0 {
		defVal = envVal
	}
	flag.StringVar(pstr, flagName, defVal, helpMessage)
}

func checkCounts(sourceCollection *mongo.Collection, targetCollection *mongo.Collection) bool {
	sourceCount, err := sourceCollection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	targetCount, err := targetCollection.CountDocuments(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}

	if sourceCount == targetCount {
		return true
	} else {
		io.WriteString(os.Stdout, fmt.Sprintf("Document counts don't match. Source: %d, Target: %d\n", sourceCount, targetCount))
		return false
	}
}

func compareIndexes(sourceCollection *mongo.Collection, targetCollection *mongo.Collection) bool {
	cursor, err := sourceCollection.Indexes().List(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	var sourceIndexes []bson.M
	if err = cursor.All(context.TODO(), &sourceIndexes); err != nil {
		panic(err)
	}

	// delete "background" key for comparison
	for _, index := range sourceIndexes {
		delete(index, "background")
	}

	cursor, err = targetCollection.Indexes().List(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	var targetIndexes []bson.M
	if err = cursor.All(context.TODO(), &targetIndexes); err != nil {
		panic(err)
	}

	// delete "background" key for comparison
	for _, index := range targetIndexes {
		delete(index, "background")
	}

	// Sort the two slices of indexes by the name, since they can be in different orders coming
	// from Indexes().List().
	// TODO: What determines the order? I deleted one and recreated and it came last in the list.
	sort.SliceStable(sourceIndexes, func(i, j int) bool {
		return sourceIndexes[i]["name"].(string) < sourceIndexes[j]["name"].(string)
	})
	sort.SliceStable(targetIndexes, func(i, j int) bool {
		return targetIndexes[i]["name"].(string) < targetIndexes[j]["name"].(string)
	})
	isEqual := cmp.Equal(sourceIndexes, targetIndexes)

	if !isEqual {
		fmt.Println("Indexes are not the same. Source indexes:")
		fmt.Println(sourceIndexes)
		fmt.Println("Target indexes:")
		fmt.Println(targetIndexes)
		fmt.Println("")
		return false
	}

	return true
}

func compareSampleContent(sourceCollection *mongo.Collection, targetCollection *mongo.Collection, randomSampleSize int) bool {
	pipeline := bson.A{bson.D{{"$sample", bson.D{{"size", randomSampleSize}}}}}
	cursor, err := sourceCollection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())
	matches := 0
	for cursor.Next(context.TODO()) {
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			log.Fatal(err)
		}

		var targetDoc bson.M
		if err = targetCollection.FindOne(context.TODO(), bson.M{"_id": doc["_id"]}).Decode(&targetDoc); err != nil {
			log.Fatal(err)
		}
		if cmp.Equal(doc, targetDoc, cmp.AllowUnexported(primitive.Decimal128{})) {
			matches++
		} else {
			fmt.Println("Documents with _id " + doc["_id"].(string) + " do not have equal content")
		}
	}

	if matches < randomSampleSize {
		return false
	}
	return true
}

func mongoAppName() string {
	return "mongocompare"
}
