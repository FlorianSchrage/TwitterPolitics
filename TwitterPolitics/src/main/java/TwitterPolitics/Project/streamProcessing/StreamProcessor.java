package TwitterPolitics.Project.streamProcessing;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.io.Files;

import TwitterPolitics.Project.batchProcessing.MongoDBConnector;
import TwitterPolitics.Project.streamIngestion.Record;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class StreamProcessor<K> {

	// private static final String HADOOP_COMMON_PATH = "C:/Users/D060237/git/BDM_Lab3_Training/src/main/resources/winutils";
	private static final String HADOOP_COMMON_PATH = System.getProperty("user.dir") + "/src/main/resources/winutils";
	public static final String TOPIC = "Politics";

	private static final int WINDOW_DURATION_SECS = 300;// 3600;
	private static final int SLIDE_DURATION_SECS = 1; // TODO: 300
	private static final double OCCURENCE_RATIO_THRESHOLD = 0.0025;

	private static SparkConf sparkConfig;
	private static JavaSparkContext sparkCtx;
	private static SQLContext sqlCtx;
	private static JavaStreamingContext jStreamCtx;

	private static List<String> initialHashtags;
	private static List<String> additionalHashtags;
	private static int initialHashtagCount;

	private static final int MONGO_QUERY_INTERVAL_MS = 1000;
	private static long lastMongoQueryTime;
	private static Map<String, JSONArray> wordTopics;
	private static String[] topicNames;

	private static int empty;
	private static int noInitial;
	private static int initial;

	public static void secondDraft() {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

		sparkCtx = getSparkContext();
		sqlCtx = new SQLContext(sparkCtx);
		jStreamCtx = new JavaStreamingContext(sparkCtx, new Duration(1000));

		jStreamCtx.checkpoint(Files.createTempDir().getAbsolutePath());
		sparkCtx.setLogLevel("ERROR");

		initialHashtags = getInitialHashtags();
		additionalHashtags = new ArrayList<>();

		// HashMap<String, Integer> topicsAndReplicas = new HashMap<String, Integer>();
		// topicsAndReplicas.put("Sample", 10);
		// JavaPairReceiverInputDStream<String, String> record = KafkaUtils.createStream(jStreamCtx, "localhost:2181", "Group1",
		// topicsAndReplicas);

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton(TOPIC);

		JavaPairInputDStream<String, String> recordStrings = KafkaUtils.createDirectStream(jStreamCtx,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		System.out.println("++++++++++ START ++++++++++");

		empty = 0;
		noInitial = 0;

		JavaPairDStream<String, Record> records = recordStrings.mapToPair(f -> {
			Record record = Record.getByJsonString(f._2);
			return new Tuple2<>(f._1, record);
		});

		// Just take Records that contain an initial hashtag
		JavaPairDStream<String, Record> recordsWithInitialHashtags = records.filter(recordTuple -> {
			// Record record = Record.getByJsonString(recordString._2);
			Record record = recordTuple._2;

			if (record.getHashtagList().isEmpty()) {
				empty++;
				// System.out.println("Tweet discarded - no hashtags (" + empty + "): " + record.getText() + "///" +
				// Arrays.toString(record.getHashtagList().toArray()));
				return false;
			}

			for (Iterator<String> iterator = record.getHashtagList().iterator(); iterator.hasNext();) {
				if (initialHashtagsContains(iterator.next())) {
					initial++;
					// System.out.println("Tweet accepted - initial hashtag found (" + initial + ")");
					return true;
				}
			}
			noInitial++;
			// System.out.println("Tweet discarded - no initial hashtags (" + noInitial + "): " + record.getText() + "///" +
			// Arrays.toString(record.getHashtagList().toArray()));
			return false;
		});

		// Start: Alle Tweets mit mindestens einem iHashtag

		// <<Initial Hashtag, Additional Hashtag>, 1>
		JavaPairDStream<Tuple2<String, String>, Integer> hashtagTuples = recordsWithInitialHashtags.flatMapToPair(recordTuple -> {
			// Record record = Record.getByJsonString(recordString._2);
			Record record = recordTuple._2;

			ArrayList<Tuple2<Tuple2<String, String>, Integer>> iHashAndAssociated = new ArrayList<>();
			ArrayList<String> initialHashtagsInThisTweet = new ArrayList<>();
			ArrayList<String> additionalHashtagsInThisTweet = new ArrayList<>();
			// JavaDStream<String> asd = initialHashtagsInThisTweet.stream();
			record.getHashtagList().stream().forEach(h -> {
				if (initialHashtagsContains(h))
					initialHashtagsInThisTweet.add(h);
				else
					additionalHashtagsInThisTweet.add(h);
			});

			if (initialHashtagsInThisTweet.isEmpty())
				throw new IllegalStateException("No initial Hashtags in this Record!");

			additionalHashtagsInThisTweet.stream().forEach(h1 -> {
				initialHashtagsInThisTweet.stream().forEach(h2 -> {
					iHashAndAssociated.add(new Tuple2<>(new Tuple2<>(h2, h1), 1));
				});
			});

			return iHashAndAssociated;
		});

		// <Initial Hashtag, 1>
		JavaPairDStream<String, Integer> initialHashtagTuples = hashtagTuples.mapToPair(tuple -> new Tuple2<>(tuple._1._1, tuple._2));

		// <Additional Hashtag, 1>
		JavaPairDStream<String, Integer> additionalHashtagTuples = hashtagTuples.mapToPair(tuple -> new Tuple2<>(tuple._1._2, tuple._2));

		JavaPairDStream<String, Integer> initialHashtagCounts = initialHashtagTuples.reduceByKeyAndWindow(
				(i1, i2) -> i1 + i2,
				(i1, i2) -> i1 - i2,
				new Duration(WINDOW_DURATION_SECS * 1000),
				new Duration(SLIDE_DURATION_SECS * 1000));

		JavaPairDStream<String, Integer> additionalHashtagCounts = additionalHashtagTuples.reduceByKeyAndWindow(
				(i1, i2) -> i1 + i2,
				(i1, i2) -> i1 - i2,
				new Duration(WINDOW_DURATION_SECS * 1000),
				new Duration(SLIDE_DURATION_SECS * 1000));

		// JavaPairDStream<Tuple2<String, String>, Integer> coOccurringHashtagCounts = hashtagTuples.
		// reduceByKeyAndWindow(
		// (i1,i2) -> i1+i2,
		// (i1,i2) -> i1-i2,
		// new Duration(WINDOW_DURATION_SECS * 1000),
		// new Duration(SLIDE_DURATION_SECS * 1000));

		JavaPairDStream<Integer, Integer> swappedInitialHashtagCounts = initialHashtagCounts.mapToPair(f -> new Tuple2<>(1, f._2));
		JavaPairDStream<Integer, Integer> initialHashtagSingleCount = swappedInitialHashtagCounts.reduceByKey((i1, i2) -> i1 + i2);

		initialHashtagSingleCount.foreachRDD(rdd -> {
			int iteration = 0;
			for (Tuple2<Integer, Integer> t : rdd.take(10)) {
				initialHashtagCount = t._2;
				// System.out.println("initialHashtagCount = " + initialHashtagCount);
				if (iteration != 0)
					throw new IllegalStateException("Count result contains more than one tuple!"); // If this exception occurs, something
																									// went wrong!
				iteration++;
			}
		});

		JavaPairDStream<String, Double> additionalHashtagOccurenceRatios = additionalHashtagCounts
				.mapToPair(f -> new Tuple2<>(f._1, (double) f._2 / (double) initialHashtagCount));

		JavaPairDStream<String, Double> filteredAdditionalHashtagOccurenceRatios = additionalHashtagOccurenceRatios
				.filter(f -> f._2 >= OCCURENCE_RATIO_THRESHOLD);

		// JavaPairDStream<Double, String> swappedCounts = additionalHashtagOccurenceRatios.mapToPair(count -> count.swap());
		// JavaPairDStream<Double, String> sortedCounts = swappedCounts.transformToPair(count -> count.sortByKey(false));
		//
		// sortedCounts.foreachRDD(rdd -> {
		// String out = "\nTop 10 additional hashtag ratios:\n";
		// for (Tuple2<Double, String> t : rdd.take(10)) {
		// out = out + t.toString() + "\n";
		// }
		// System.out.println(out);
		// });

		filteredAdditionalHashtagOccurenceRatios.foreachRDD(rdd -> {
			additionalHashtags = new ArrayList<>();
			rdd.foreach(tuple -> additionalHashtags.add(tuple._1));
		});

		JavaPairDStream<String, Record> recordsWithValidAdditionalHashtags = records.filter(recordTuple -> {
			// Record record = Record.getByJsonString(recordString._2);
			Record record = recordTuple._2;

			for (Iterator<String> iterator = record.getHashtagList().iterator(); iterator.hasNext();) {
				if (additionalHashtagsContains(iterator.next()))
					return true;
			}
			return false;
		});

		JavaPairDStream<String, Record> validRecords = recordsWithInitialHashtags.union(recordsWithValidAdditionalHashtags);

		JavaPairDStream<String, Record> cleanValidRecords = validRecords
				.mapToPair(recordTuple -> {
					// Record record = Record.getByJsonString(recordString._2);
					Record record = recordTuple._2;

					List<String> recordText = new ArrayList<>();
					recordText.add(record.getText());
					JavaRDD<String> stringRdd = sparkCtx.parallelize(recordText);
					JavaRDD<Row> rowRdd = stringRdd.map(sentence -> RowFactory.create(sentence));
					StructType schema = new StructType(new StructField[] {
							new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
					});

					DataFrame untokenized = sqlCtx.createDataFrame(rowRdd, schema);
					Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
					DataFrame tokenized = tokenizer.transform(untokenized);

					if (tokenized.count() > 1)
						throw new IllegalStateException("Tokenized result contains more than one tuple!");

					Row[] row = tokenized.take(1);
					List<String> tokens = row[0].getList(1);

					tokens = tokens.stream()
							.map(f -> f.replaceAll("[^A-Za-z0-9]", ""))
							.filter(f -> !(f == null || f.isEmpty() || f.matches("^[^A-Za-z0-9]{1}$")))
							.collect(Collectors.toList());

					// Re-create RDD because the Tokenizer was too dumb and required some post-processing on the tokenized list before
					List<List<String>> recordTokens = new ArrayList<>();
					recordTokens.add(tokens);
					JavaRDD<List<String>> stringListRdd = sparkCtx.parallelize(recordTokens);
					JavaRDD<Row> rowRdd2 = stringListRdd.map(array -> RowFactory.create(array));
					StructType schema2 = new StructType(new StructField[] {
							new StructField("with_stopwords", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
					});

					DataFrame withStopwords = sqlCtx.createDataFrame(rowRdd2, schema2);
					StopWordsRemover remover = new StopWordsRemover().setInputCol("with_stopwords").setOutputCol("without_stopwords");
					String[] stopWords = remover.getStopWords();
					String[] stopWordsExtended = Arrays.copyOf(stopWords, stopWords.length + 1);
					stopWordsExtended[stopWordsExtended.length - 1] = "rt"; // rt is contained in the Text whenever it is a retweet
					remover.setStopWords(stopWordsExtended);
					DataFrame withoutStopwords = remover.transform(withStopwords);

					if (withoutStopwords.count() > 1)
						throw new IllegalStateException("Stopword-removal result contains more than one tuple!");

					Row[] row2 = withoutStopwords.take(1);
					List<String> tokens2 = row2[0].getList(1);

					record.setCleanedWords(tokens2);

					// System.out.println("Text \"" + record.getText() + "\" -> " + tokens2);
					// for (Iterator<String> iterator = tokens2.iterator(); iterator.hasNext();) {
					// String token = (String) iterator.next();
					//
					// System.out.print(token + ", ");
					// }
					// System.out.println();

					return new Tuple2<>(recordTuple._1, record);
				});

		cleanValidRecords.cache();
		MongoDBConnector.saveToMongo(cleanValidRecords, MongoDBConnector.Collections.TWEETS);

		JavaPairDStream<String, Record> recordWithTopics = cleanValidRecords
				.mapToPair(recordTuple -> {
					Record record = recordTuple._2;
					List<String> words = record.getCleanedWords();

					// Query every minute
					if (wordTopics == null || lastMongoQueryTime == 0 || (lastMongoQueryTime + MONGO_QUERY_INTERVAL_MS) < System.currentTimeMillis()) {

						JavaRDD<Document> wordTopicsRDD = MongoDBConnector.getRDDs(MongoDBConnector.Collections.RESULTS);
						List<Document> wordTopicsList = wordTopicsRDD.collect();
						wordTopics = new HashMap<>();
						for (Iterator<Document> iterator = wordTopicsList.iterator(); iterator.hasNext();) {
							Document document = iterator.next();

							try {
								String key = document.get("_id").toString();
								// ArrayList<String> abc = (ArrayList<String>) document.get(MongoDBConnector.RECORD);

								// System.out.println("document: " + document.toJson());
								// for (Iterator iterator2 = abc.iterator(); iterator2.hasNext();) {
								// System.out.println("Element: " + iterator2.next());
								//
								// }
								JSONArray value = new JSONArray(document.getString(MongoDBConnector.RECORD));
								wordTopics.put(key, value);
							} catch (JSONException e) {
								System.out.println("Error for document: " + document.toJson());
								e.printStackTrace();
							}
						}

						JavaRDD<Document> topicsRDD = MongoDBConnector.getRDDs(MongoDBConnector.Collections.TOPICS);
						List<Document> topicsList = topicsRDD.collect();
						topicNames = new String[topicsList.size()];

						for (Iterator<Document> iterator = topicsList.iterator(); iterator.hasNext();) {
							Document document = iterator.next();

							int index = Integer.parseInt(document.get("_id").toString());
							String name = document.getString(MongoDBConnector.TOPIC);

							if (topicNames[index] != null)
								throw new IllegalStateException("Duplicate Topics in Topic List!");

							topicNames[index] = name;
						}

						lastMongoQueryTime = System.currentTimeMillis();
					}

					double[] weightsPerTopic = new double[topicNames.length];

					for (Iterator<String> iterator = words.iterator(); iterator.hasNext();) {
						String word = iterator.next();
						JSONArray weights = wordTopics.get(word);

						if (weights == null)
							continue;

						double[] weightsPerTopicForThisWord = new double[topicNames.length];

						for (int i = 0; i < weights.length(); i++) {
							JSONObject topicAndWeight = weights.getJSONObject(i);
							int index = Integer.parseInt(topicAndWeight.keys().next());

							if (weightsPerTopicForThisWord[index] != 0.0)
								throw new IllegalStateException("Duplicate Topics in Result!");

							weightsPerTopicForThisWord[i] = topicAndWeight.getDouble((index + ""));
							Arrays.setAll(weightsPerTopic, j -> weightsPerTopic[j] + weightsPerTopicForThisWord[j]);
						}
					}

					double max = 0.0;
					int topicNumber = 0;
					for (int i = 1; i < weightsPerTopic.length; i++) {
						if (weightsPerTopic[i] > max) {
							topicNumber = i;
							max = weightsPerTopic[i];
						}
					}

					record.setTopic(topicNames[topicNumber]);

					return new Tuple2<>(recordTuple._1, record);
				});

		recordWithTopics.foreachRDD(f -> {
			f.foreach(g -> {
				System.out.println(g._1 + ": " + g._2.getText() + "\n \t Topic: " + g._2.getTopic());
			});
		});

		// cleanValidRecords.print();

		// cleanValidRecords.foreachRDD(rdd -> {
		// rdd.foreach(record -> {
		// System.out.println("Record " + record._1);
		// System.out.println("\t" + "Language: " + record._2.getLanguage());
		// System.out.println("\t" + "Text: " + record._2.getText().replaceAll("\\s", " "));
		// System.out.print("\t" + "Cleaned: ");
		// boolean first = true;
		// for (Iterator<String> iterator = record._2.getCleanedWords().iterator(); iterator.hasNext();) {
		// String word = (String) iterator.next();
		// if(first)
		// System.out.print(word);
		// else
		// System.out.print(", " + word);
		// first = false;
		// }
		// System.out.println();
		// System.out.print("\t" + "Hashtag List: ");
		// first = true;
		// for (Iterator<String> iterator = record._2.getHashtagList().iterator(); iterator.hasNext();) {
		// String hashtag = (String) iterator.next();
		// if(first)
		// System.out.print(hashtag);
		// else
		// System.out.print(", " + hashtag);
		// first = false;
		// }
		// System.out.println();
		// });
		// });

		// hashtagsCounts.print();

		// record.mapToPair(f -> {
		// Record thisRecord = Record.getByJsonString(f._2);
		// System.out.println("Processing " + f._2);
		// System.out.println("Text: " + thisRecord.getText());
		// System.out.println("Language: " + thisRecord.getLanguage());
		// if(thisRecord.getLocation() != null)
		// System.out.println("Location: " + thisRecord.getLocation().toString());
		// if(thisRecord.getPlace() != null)
		// System.out.println("Place: " + thisRecord.getPlace().toString());
		// return new Tuple2<String, String>(f._1 + "/////" + f._2 ,"Hello World!");
		// }).print();

		jStreamCtx.start();
		jStreamCtx.awaitTermination();
	}

	public static List<String> getInitialHashtags() {
		List<String> hashtags = new ArrayList<>();
		try {

			java.nio.file.Files
					.lines(new File(System.getProperty("user.dir") + "\\src\\main\\resources\\initialHashtags.txt").toPath(), Charset.forName("UTF-8"))
					.forEach(s -> hashtags.addAll(Arrays.asList(s.toLowerCase().split("#"))));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new ArrayList<>(new HashSet<>(hashtags));
	}

	private static boolean initialHashtagsContains(String contains) {
		return initialHashtags.contains(contains.toLowerCase());
	}

	private static boolean additionalHashtagsContains(String contains) {
		return additionalHashtags.contains(contains.toLowerCase());
	}

	public static void main(String[] args) {
		System.out.println("Consumer running...");

		StreamProcessor.secondDraft();
	}

	public static JavaSparkContext getSparkContext() {
		String connectionString = "mongodb://127.0.0.1/" + MongoDBConnector.DB_NAME + "."
				+ TwitterPolitics.Project.batchProcessing.MongoDBConnector.Collections.TWEETS;
		if (sparkCtx != null) {
			return sparkCtx;
		}

		SparkConf sc = new SparkConf()
				.setAppName("TwitterPolitics")
				.setMaster("local[*]")
				.set("spark.mongodb.input.uri", connectionString)
				.set("spark.mongodb.output.uri", connectionString);
		sparkCtx = new JavaSparkContext(sc);

		return sparkCtx;
	}

}
