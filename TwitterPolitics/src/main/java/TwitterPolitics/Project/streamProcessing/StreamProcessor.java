package TwitterPolitics.Project.streamProcessing;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class StreamProcessor {
	
	public void secondDraft()
	{
        SparkConf sparkConfig = new SparkConf().setAppName("TwitterPolitics").setMaster("local[*]");
        JavaSparkContext sparkCtx = new JavaSparkContext(sparkConfig);
        JavaStreamingContext jStreamCtx = new JavaStreamingContext(sparkCtx, new Duration(1000));

        //localhost:2181
        //GroupId muss nur unique sein
        KafkaUtils.createStream(jStreamCtx, "localhost:2181", "Group1", new HashMap<String, Integer>())
	}

}
