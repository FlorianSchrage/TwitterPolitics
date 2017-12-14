package TwitterPolitics.Project.streamProcessing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import TwitterPolitics.Project.streamIngestion.KafkaTwitterIngestion;
import scala.Tuple2;

public class StreamProcessor {
	
	private static final String HADOOP_COMMON_PATH = "C:/Users/D060237/git/BDM_Lab3_Training/src/main/resources/winutils";
	
	public void secondDraft()
	{
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        SparkConf sparkConfig = new SparkConf().setAppName("TwitterPolitics").setMaster("local[*]");
        JavaSparkContext sparkCtx = new JavaSparkContext(sparkConfig);
        JavaStreamingContext jStreamCtx = new JavaStreamingContext(sparkCtx, new Duration(1000));

        HashMap<String, Integer> topicsAndReplicas = new HashMap<String, Integer>();
        topicsAndReplicas.put("Sample", 1);
        
        //localhost:2181
        //GroupId muss nur unique sein
        JavaPairReceiverInputDStream<String, String> record = KafkaUtils.createStream(jStreamCtx, "localhost:2181", "Group1", topicsAndReplicas);
        
        record.mapToPair(f -> new Tuple2<String, String>(f._1 + "/////" + f._2 ,"Hello World!")).print();
	}
	
	public static void main(String[] args) {
		System.out.println("Consumer running...");
		
		StreamProcessor processor = new StreamProcessor();
		processor.secondDraft();
	}
}
