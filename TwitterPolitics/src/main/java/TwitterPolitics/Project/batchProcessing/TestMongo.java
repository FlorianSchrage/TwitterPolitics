/**
 * 
 */
package TwitterPolitics.Project.batchProcessing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.spark.MongoSpark;

public class TestMongo {

	public static final String RECORD = "record";
	public static final String DB_NAME = "twitterTopics";
	static JavaSparkContext jsc;

	public enum Collections {
		TWEETS("tweets"), RESULTS("results");

		Collections(String name) {
			collectionName = name;
		}

		public String getCollectionName() {
			return collectionName;
		}

		private String collectionName;
	}

	private static MongoClient getMongoDBClient() {
		MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017");
		return new MongoClient(connectionString);
	}

	private static MongoCollection<Document> getCollection(Collections collection) {
		return getMongoDBClient().getDatabase(DB_NAME).getCollection(collection.getCollectionName());
	}

	private static void removeCollection(Collections collection) {
		getCollection(collection).drop();
	}

	@SuppressWarnings("deprecation")
	public static void saveToMongo(JavaPairDStream<String, String> stream, Collections collection) {
		removeCollection(collection);
		stream.toJavaDStream().map(f -> new Document(RECORD, f._2)).foreachRDD(new Function<JavaRDD<Document>, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<Document> data) throws Exception {
				System.out.println("Before insert: " + getCollection(collection).count() + " documents found. " + data.count() + " data elements");
				if (data != null && data.count() > 0) {
					getCollection(collection).insertMany(data.collect());
					System.out.println("Inserted Data Done");
				}
				System.out.println("After insert: " + getCollection(collection).count() + " documents found");
				return null;
			}

		});

	}

	public static void printAllData(Collections collection) {
		System.out.println("Print all data in " + collection.getCollectionName());
		getCollection(collection).find().forEach((Block<Document>) d -> {
			System.out.println(d.toJson());
		});
	}

	public static JavaRDD<Document> getRDDs(Collections collection) {
		return MongoSpark.load(getSparkContext(collection));
	}

	/**
	 * @param collection
	 * @return
	 */
	public static JavaSparkContext getSparkContext(Collections collection) {
		if (jsc != null)
			return jsc;

		String connectionString = "mongodb://127.0.0.1/" + DB_NAME + "." + collection.getCollectionName();
		SparkConf sc = new SparkConf()
				.setMaster("local")
				.setAppName("MongoSparkConnector")
				.set("spark.mongodb.input.uri", connectionString)
				.set("spark.mongodb.output.uri", connectionString);

		jsc = new JavaSparkContext(sc);
		return jsc;
	}

	// TODO: currently not working ...
	// public static List<Record> getRecords(Collections collection) {
	// System.out.println("Get records from " + collection.getCollectionName());
	// List<Record> records = new ArrayList<>();
	// getCollection(collection).find().forEach((Block<Document>) d -> {
	// Record currentRecord = Record.getByJsonString(d.toJson());
	//
	// if (currentRecord != null) {
	// records.add(currentRecord);
	// } else {
	// System.out.println("ERROR: Was not able to create Record from: " + d.toJson());
	// }
	//
	// });
	// return records;
	// }
}
