/**
 * 
 */
package TwitterPolitics.Project.batchProcessing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bson.Document;
import org.json.JSONArray;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import TwitterPolitics.Project.streamIngestion.Record;
import TwitterPolitics.Project.streamProcessing.StreamProcessor;

public class MongoDBConnector {

	public static final String RECORD = "record";
	public static final String TOPIC = "topic";
	public static final String DB_NAME = "twitterTopics";
	public static final String DELETION_COUNTER = "deletionCount";
	public static final int SHARE_OF_DELETION = 3;

	public enum Collections {
		TWEETS("tweets"), RESULTS("results"), TOPICS("topics");

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
	public static void saveToMongo(JavaPairDStream<String, Record> records, Collections collection) {
		// TODO: remove remove ;) and improve save method
		removeCollection(collection);
		records.toJavaDStream().map(f -> new Document(RECORD, f._2.getCleanedWords()).append(DELETION_COUNTER, System.currentTimeMillis()))
				.foreachRDD(new Function<JavaRDD<Document>, Void>() {
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
		System.out.println("Print all data in " + collection.getCollectionName() + " Documents: " + getCollection(collection).count());
		getCollection(collection).find().forEach((Block<Document>) d -> {
			System.out.println(d.toJson());
		});
	}

	public static JavaRDD<Document> getRDDs(Collections collection) {
		Map<String, String> readOverrides = new HashMap<>();
		readOverrides.put("collection", collection.getCollectionName());

		return MongoSpark.load(StreamProcessor.getSparkContext(), ReadConfig.create(StreamProcessor.getSparkContext()).withOptions(readOverrides));
	}

	/**
	 * @param collection
	 * @return
	 */
	// public static JavaSparkContext getSparkContext(Collections collection) {
	// if (jsc != null)
	// return jsc;

	// String connectionString = "mongodb://127.0.0.1/" + DB_NAME + "." + collection.getCollectionName();
	// SparkConf sc = new SparkConf()
	// .setMaster("local")
	// .setAppName("MongoSparkConnector")
	// .set("spark.mongodb.input.uri", connectionString)
	// .set("spark.mongodb.output.uri", connectionString);
	//
	// jsc = new JavaSparkContext(sc);

	// StreamProcessor.getSparkConfig()
	// .set("spark.mongodb.input.uri", connectionString)
	// .set("spark.mongodb.output.uri", connectionString);
	//
	// return jsc;
	// }

	/**
	 * @param wordsWithTopicRelatedValues
	 * @param results
	 */
	public static void saveToMongo(HashMap<String, JSONArray> data, Collections collection) {

		System.out.println("Before insert: " + getCollection(collection).count() + " documents found");
		List<Document> documents = new ArrayList<>();
		data.forEach((s, j) -> documents.add(new Document(RECORD, j.toString()).append("_id", s)));
		getCollection(collection).insertMany(documents);
		System.out.println("After insert: " + getCollection(collection).count() + " documents found");
	}

	public static void saveTopicsToMongo(HashMap<Integer, List<String>> data) {

		System.out.println("Before insert: " + getCollection(Collections.TOPICS).count() + " documents found");

		List<Document> documents = new ArrayList<>();
		data.forEach((i, l) -> {
			final StringBuffer words = new StringBuffer();
			l.forEach(w -> words.append(w + ", "));
			String description = words.toString().substring(0, words.length() - 2);
			documents.add(new Document(TOPIC, description).append("_id", i));
		});
		removeCollection(Collections.TOPICS);
		getCollection(Collections.TOPICS).insertMany(documents);
		System.out.println("After insert: " + getCollection(Collections.TOPICS).count() + " documents found");
	}

	/**
	 * 
	 */
	public static void dropTweets() {
		// printAllData(Collections.TWEETS);

		Random generator = new Random();
		OptionalInt divisor = generator.ints(SHARE_OF_DELETION / 2 + 1, SHARE_OF_DELETION + SHARE_OF_DELETION / 2 + 1).findFirst();
		int remainder = generator.nextInt(divisor.getAsInt());
		// System.out.println("Random remainder: " + remainder + divisor.getAsInt());
		getCollection(Collections.TWEETS).deleteMany(Filters.mod(DELETION_COUNTER, divisor.getAsInt(), remainder));
		// printAllData(Collections.TWEETS);
	}

}
