package TwitterPolitics.Project.streamProcessing;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import TwitterPolitics.Project.batchProcessing.TestMongo;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class StreamProcessor {

	private static final String HADOOP_COMMON_PATH = "C:/Users/D060237/git/BDM_Lab3_Training/src/main/resources/winutils";
	public static final String TOPIC = "Politics";

	public void secondDraft() {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
		SparkConf sparkConfig = new SparkConf()
				.setAppName("TwitterPolitics")
				.setMaster("local[*]");
		JavaSparkContext sparkCtx = new JavaSparkContext(sparkConfig);
		JavaStreamingContext jStreamCtx = new JavaStreamingContext(sparkCtx, new Duration(1000));

		// HashMap<String, Integer> topicsAndReplicas = new HashMap<String, Integer>();
		// topicsAndReplicas.put("Sample", 10);

		// localhost:2181
		// GroupId muss nur unique sein
		// JavaPairReceiverInputDStream<String, String> record = KafkaUtils.createStream(jStreamCtx, "localhost:2181", "Group1",
		// topicsAndReplicas);

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton(TOPIC);

		JavaPairInputDStream<String, String> record = KafkaUtils.createDirectStream(jStreamCtx,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		System.out.println("I will print...");
		record.print();

		JavaPairDStream<String, String> stream = record.mapToPair(f -> {
			// Record thisRecord = Record.getByJsonString(f._2);
			System.out.println("Processing " + f._2);
			// System.out.println("Text: " + thisRecord.getText());
			// System.out.println("Language: " + thisRecord.getLanguage());
			// System.out.println("Location: " + thisRecord.getLocation().toString());
			// System.out.println("Place: " + thisRecord.getPlace().toString());
			return new Tuple2<>(f._1, f._2);
		});

		TestMongo.saveToMongo(stream, TestMongo.Collections.TWEETS);
		System.out.println("TEststdasgsdg");
		TestMongo.readAllData(TestMongo.Collections.TWEETS);

		jStreamCtx.start();
		jStreamCtx.awaitTermination();
	}

	public static void main(String[] args) {
		System.out.println("Consumer running...");

		StreamProcessor processor = new StreamProcessor();
		processor.secondDraft();
	}
}
