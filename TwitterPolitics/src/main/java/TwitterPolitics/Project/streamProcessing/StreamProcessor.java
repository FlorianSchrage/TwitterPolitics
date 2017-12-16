package TwitterPolitics.Project.streamProcessing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.io.Files;

import TwitterPolitics.Project.streamIngestion.KafkaTwitterIngestion;
import TwitterPolitics.Project.streamIngestion.Record;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class StreamProcessor {
	
	private static final String HADOOP_COMMON_PATH = "C:/Users/D060237/git/BDM_Lab3_Training/src/main/resources/winutils";
	public static final String TOPIC = "Politics";
	
	private static final int WINDOW_DURATION_SECS = 300;//3600;
	private static final int SLIDE_DURATION_SECS = 1;
	
	private List<String> initialHashtags;
	
	public void secondDraft()
	{
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        SparkConf sparkConfig = new SparkConf().setAppName("TwitterPolitics").setMaster("local[*]");
        JavaSparkContext sparkCtx = new JavaSparkContext(sparkConfig);
        JavaStreamingContext jStreamCtx = new JavaStreamingContext(sparkCtx, new Duration(1000));
        jStreamCtx.checkpoint(Files.createTempDir().getAbsolutePath());

//        HashMap<String, Integer> topicsAndReplicas = new HashMap<String, Integer>();
//        topicsAndReplicas.put("Sample", 10);
        
        //localhost:2181
        //GroupId muss nur unique sein
//        JavaPairReceiverInputDStream<String, String> record = KafkaUtils.createStream(jStreamCtx, "localhost:2181", "Group1", topicsAndReplicas);
        

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton(TOPIC);

        JavaPairInputDStream<String, String> records = KafkaUtils.createDirectStream(jStreamCtx,
        		String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);


        System.out.println("++++++++++ START ++++++++++");
        
        //Just take Records that contain hashtags
        JavaPairDStream<String, String> recordsWithHashtags =
        		records.filter(recordString -> {
        			Record record = Record.getByJsonString(recordString._2);
        			return !record.getHashtagList().isEmpty();
        		});
        
        
        //Start: Alle Tweets mit mindestens einem iHashtag
        
        JavaPairDStream<Tuple2<String, String>, Integer> hashtagTuples = recordsWithHashtags.
        		flatMapToPair(recordString -> {
        			Record record = Record.getByJsonString(recordString._2);      
        			
        			ArrayList<Tuple2<Tuple2<String, String>, Integer>> iHashAndAssociated = new ArrayList<>();
        			ArrayList<String> initialHashtagsInThisTweet = new ArrayList<>();
        			ArrayList<String> additionalHashtagsInThisTweet = new ArrayList<>();
        			record.getHashtagList().stream().forEach(h -> {
        				if(initialHashtags.contains(h))
        					initialHashtagsInThisTweet.add(h);
        				else
        					additionalHashtagsInThisTweet.add(h);
        			});
        			
        			if(initialHashtagsInThisTweet.isEmpty())
        				throw new IllegalStateException("No initial Hashtags in this Record!");

        			additionalHashtagsInThisTweet.stream().forEach(h1 -> {
        				initialHashtagsInThisTweet.stream().forEach(h2 -> {
        					iHashAndAssociated.add(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(h2, h1),1));
        				});
        			});
        			
        			return iHashAndAssociated;
        		});
        
//        JavaPairDStream<String, Integer> hashtags = recordsWithHashtags.
//                flatMapToPair(recordString -> {
//                	Record record = Record.getByJsonString(recordString._2);        			
//        			
//        			ArrayList<Tuple2<String, Integer>> recordHashtags = new ArrayList<>();
//        			record.getHashtagList().stream().forEach(h -> recordHashtags.add(new Tuple2<String, Integer>(h, 1)));
//        
//                	return recordHashtags;
//                });
        
        
        JavaPairDStream<Tuple2<String, String>, Integer> coOccurringHashtagCounts = hashtagTuples.
                reduceByKeyAndWindow(
                        (i1,i2) -> i1+i2,
                        (i1,i2) -> i1-i2,
                        new Duration(WINDOW_DURATION_SECS * 1000),
                        new Duration(SLIDE_DURATION_SECS * 1000));
        
        
        
//        hashtagsCounts.print();
        
        
//        record.mapToPair(f -> {
//        	Record thisRecord = Record.getByJsonString(f._2);
//        	System.out.println("Processing " + f._2);
//        	System.out.println("Text: " + thisRecord.getText());
//        	System.out.println("Language: " + thisRecord.getLanguage());
//        	if(thisRecord.getLocation() != null)
//        	System.out.println("Location: " + thisRecord.getLocation().toString());
//        	if(thisRecord.getPlace() != null)
//        	System.out.println("Place: " + thisRecord.getPlace().toString());
//        	return new Tuple2<String, String>(f._1 + "/////" + f._2 ,"Hello World!");
//        }).print();
        
        jStreamCtx.start();
        jStreamCtx.awaitTermination();
	}
	
	public static void main(String[] args) {
		System.out.println("Consumer running...");
		
		StreamProcessor processor = new StreamProcessor();
		processor.secondDraft();
	}
}
