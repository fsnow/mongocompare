package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/google/go-cmp/cmp"
	flag "github.com/spf13/pflag"
)

func main() {
	var sourceURI, sourceUsername, sourcePassword, sourceDatabase, sourceCollName string
	flag.StringVar(&sourceURI, "sourceURI", "", "source connection string")
	flag.StringVar(&sourceUsername, "sourceUsername", "", "source username")
	flag.StringVar(&sourcePassword, "sourcePassword", "", "source password")
	flag.StringVar(&sourceDatabase, "sourceDatabase", "", "source database")
	flag.StringVar(&sourceCollName, "sourceCollName", "", "source collection")

	var targetURI, targetUsername, targetPassword, targetDatabase, targetCollName string
	flag.StringVar(&targetURI, "targetURI", "", "target connection string")
	flag.StringVar(&targetUsername, "targetUsername", "", "target username")
	flag.StringVar(&targetPassword, "targetPassword", "", "target password")
	flag.StringVar(&targetDatabase, "targetDatabase", "", "target database")
	flag.StringVar(&targetCollName, "targetCollName", "", "target collection")

	var randomSampleSize, firstIdsCount, lastIdsCount int
	flag.IntVar(&randomSampleSize, "randomSampleSize", 100, "random sample size used for content comparison")
	flag.IntVar(&firstIdsCount, "firstIdsCount", 100, "number of _id values to compare from beginning")
	flag.IntVar(&lastIdsCount, "lastIdsCount", 100, "number of _id values to compare from end")

	flag.Parse()

	// get any values from environment variables that were not set on the command line
	stringFromEnvVar(&sourceURI, "SOURCE_URI", "sourceURI")
	stringFromEnvVar(&sourceUsername, "SOURCE_USERNAME", "sourceUsername")
	stringFromEnvVar(&sourcePassword, "SOURCE_PASSWORD", "sourcePassword")
	stringFromEnvVar(&sourceDatabase, "SOURCE_DATABASE", "sourceDatabase")
	stringFromEnvVar(&sourceCollName, "SOURCE_COLLECTION", "sourceCollName")

	stringFromEnvVar(&targetURI, "TARGET_URI", "targetURI")
	stringFromEnvVar(&targetUsername, "TARGET_USERNAME", "targetUsername")
	stringFromEnvVar(&targetPassword, "TARGET_PASSWORD", "targetPassword")
	stringFromEnvVar(&targetDatabase, "TARGET_DATABASE", "targetDatabase")
	stringFromEnvVar(&targetCollName, "TARGET_COLLECTION", "targetCollName")

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
	compareFirstIdsResult := compareIds(sourceCollection, targetCollection, firstIdsCount, 1)
	compareLastIdsResult := compareIds(sourceCollection, targetCollection, lastIdsCount, -1)

	if checkCountsResult && indexCompareResult && sampleContentResult && compareFirstIdsResult && compareLastIdsResult {
		fmt.Println("Passed all validation checks")
		os.Exit(0)
	} else {
		fmt.Println("Some validation checks failed. See above.")

		exitVal := 0
		if !checkCountsResult {
			exitVal += 1
		}
		if !indexCompareResult {
			exitVal += 2
		}
		if !sampleContentResult {
			exitVal += 4
		}
		if !compareFirstIdsResult {
			exitVal += 8
		}
		if !compareLastIdsResult {
			exitVal += 16
		}
		os.Exit(exitVal)
	}
	fmt.Println("")
}

func stringFromEnvVar(pstr *string, envVar string, clArg string) {
	if len(*pstr) == 0 {
		*pstr = os.Getenv(envVar)
	}

	if len(*pstr) == 0 {
		fmt.Println("Set command-line arg " + clArg + " or environment variable " + envVar)
	}
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
	// get source indexes as bson.D
	cursor, err := sourceCollection.Indexes().List(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	var sourceIndexes []bson.D
	if err = cursor.All(context.TODO(), &sourceIndexes); err != nil {
		panic(err)
	}

	// A map where the keys are index names and the values are the key document.
	// We need to compare the key document as a bson.D to preserve the order.
	// The default index name is a good proxy for order, but an edge case is where
	// the index is manually named incorrectly.
	srcNameKeyMap := map[string]bson.D{}
	for _, index := range sourceIndexes {
		var name string
		var key bson.D
		for _, kv := range index {
			if kv.Key == "key" {
				key = kv.Value.(bson.D)
			} else if kv.Key == "name" {
				name = kv.Value.(string)
			}
		}
		srcNameKeyMap[name] = key
	}

	// Other than the comparison of the "key" document where order matters,
	// we will compare index docs as a bson.M map
	sourceIndexMaps := make([]bson.M, len(sourceIndexes))
	for i, doc := range sourceIndexes {
		sourceIndexMaps[i] = doc.Map()
	}
	// delete "background" key from source maps
	for _, index := range sourceIndexMaps {
		delete(index, "background")
	}

	// get target indexes as bson.D
	cursor, err = targetCollection.Indexes().List(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	var targetIndexes []bson.D
	if err = cursor.All(context.TODO(), &targetIndexes); err != nil {
		panic(err)
	}

	// convert target indexes to bson.M
	targetIndexMaps := make([]bson.M, len(targetIndexes))
	for i, doc := range targetIndexes {
		targetIndexMaps[i] = doc.Map()
	}
	// delete "background" key from target maps
	for _, index := range targetIndexMaps {
		delete(index, "background")
	}

	// name/key map for target indexes
	tgtNameKeyMap := map[string]bson.D{}
	for _, index := range targetIndexes {
		var name string
		var key bson.D
		for _, kv := range index {
			if kv.Key == "key" {
				key = kv.Value.(bson.D)
			} else if kv.Key == "name" {
				name = kv.Value.(string)
			}
		}
		tgtNameKeyMap[name] = key
	}

	// Sort the two slices of maps by the name, since they can be in different orders coming
	// from Indexes().List().
	// TODO: What determines the order? I deleted one and recreated and it came last in the list.
	sort.SliceStable(sourceIndexMaps, func(i, j int) bool {
		return sourceIndexMaps[i]["name"].(string) < sourceIndexMaps[j]["name"].(string)
	})
	sort.SliceStable(targetIndexMaps, func(i, j int) bool {
		return targetIndexMaps[i]["name"].(string) < targetIndexMaps[j]["name"].(string)
	})

	// compare the indexes as sorted slices of maps, minus the deleted "background" vals
	isEqual := cmp.Equal(sourceIndexMaps, targetIndexMaps)

	// compare the index keys as bson.D because order matters
	keysMatch := true
	if isEqual {
		for name, srcKey := range srcNameKeyMap {
			tgtKey := tgtNameKeyMap[name]
			if !cmp.Equal(srcKey, tgtKey) {
				keysMatch = false
				fmt.Println("Index key comparison failed on " + name)
			}
		}
	}

	if !isEqual || !keysMatch {
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
	if randomSampleSize <= 0 {
		return true
	}
	pipeline := bson.A{bson.D{{"$sample", bson.D{{"size", randomSampleSize}}}}}
	cursor, err := sourceCollection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())
	matches := 0
	docCount := 0
	for cursor.Next(context.TODO()) {
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			log.Fatal(err)
		}

		var targetDoc bson.M
		if err = targetCollection.FindOne(context.TODO(), bson.M{"_id": doc["_id"]}).Decode(&targetDoc); err != nil {
			log.Fatal(err)
		}

		docCount++
		if cmp.Equal(doc, targetDoc, cmp.AllowUnexported(primitive.Decimal128{})) {
			matches++
		} else {
			fmt.Println("Documents with _id " + doc["_id"].(string) + " do not have equal content")
		}
	}

	if matches < docCount {
		return false
	}
	return true
}

func compareIds(sourceCollection *mongo.Collection, targetCollection *mongo.Collection, ids int, direction int) bool {
	if ids <= 0 {
		return true
	}
	query := bson.D{}
	projection := bson.D{{"_id", 1}}
	sort := bson.D{{"_id", direction}}
	srcCursor, err := sourceCollection.Find(context.TODO(), query, options.Find().SetProjection(projection), options.Find().SetSort(sort))
	if err != nil {
		log.Fatal(err)
	}
	defer srcCursor.Close(context.TODO())
	tgtCursor, err := targetCollection.Find(context.TODO(), query, options.Find().SetProjection(projection), options.Find().SetSort(sort))
	if err != nil {
		log.Fatal(err)
	}
	defer tgtCursor.Close(context.TODO())

	count := 0
	for {
		srcHasNext := srcCursor.Next(context.TODO())
		tgtHasNext := tgtCursor.Next(context.TODO())

		var srcDoc, tgtDoc bson.M
		var src_id, tgt_id interface{}
		if srcHasNext {
			srcCursor.Decode(&srcDoc)
			src_id = srcDoc["_id"]
		}
		if tgtHasNext {
			tgtCursor.Decode(&tgtDoc)
			tgt_id = tgtDoc["_id"]
		}

		if srcHasNext && tgtHasNext {
			//fmt.Println("Src: " + src_id + ", Tgt: " + tgt_id)
			if !cmp.Equal(src_id, tgt_id, cmp.AllowUnexported(primitive.Decimal128{})) {
				fmt.Print("_id mismatch, iteration " + strconv.Itoa(count) + ", sort " + strconv.Itoa(direction))
				fmt.Print(", Source _id: ")
				fmt.Print(src_id)
				fmt.Print(", Target _id: ")
				fmt.Println(tgt_id)
				return false
			}
		} else if srcHasNext {
			fmt.Print("Target collection is missing _id ")
			fmt.Print(src_id)
			fmt.Println(" (end)")
			return false
		} else if tgtHasNext {
			fmt.Print("Source collection is missing _id ")
			fmt.Print(tgt_id)
			fmt.Println(" (end)")
			return false
		} else {
			break
		}

		count++
		if count >= ids {
			break
		}
	}
	return true
}

func mongoAppName() string {
	return "mongocompare"
}
