/**
 * 
 */
package TwitterPolitics.Project.batchProcessing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;

public class TestMongo {

	public static final String DB_NAME = "twitterTopics";

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

	@SuppressWarnings("deprecation")
	public static void saveToMongo(JavaPairDStream<String, String> stream, Collections collection) {
		stream.toJavaDStream().map(f -> new Document(f._1, f._2)).foreachRDD(new Function<JavaRDD<Document>, Void>() {
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
