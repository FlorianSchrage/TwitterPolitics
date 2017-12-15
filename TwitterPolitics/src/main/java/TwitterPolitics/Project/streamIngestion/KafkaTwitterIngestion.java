package TwitterPolitics.Project.streamIngestion;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import TwitterPolitics.Project.streamProcessing.StreamProcessor;


public class KafkaTwitterIngestion
{
	private static final String TWITTER_CONFIG_FILE_PATH = "src/main/resources/twitter_configuration.txt";
	private static final int QUEUE_CAPACITY = 1000;
	
	private LinkedBlockingQueue<Status> queue;
	
	public void firstDraft() throws Exception {
	      queue = new LinkedBlockingQueue<Status>(QUEUE_CAPACITY);
	      
	      String[] twitterConfigs = getTwitterConfigs();
	      
	      String consumerKey = twitterConfigs[0];
	      String consumerSecret = twitterConfigs[1];
	      String accessToken = twitterConfigs[2];
	      String accessTokenSecret = twitterConfigs[3];
	      String topicName = StreamProcessor.TOPIC;
//	      String[] topicNameArr = { StreamProcessor.TOPIC };
//	      String[] arguments = args.clone();
//	      String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);

	      ConfigurationBuilder cb = new ConfigurationBuilder();
	      cb.setDebugEnabled(true)
	         .setOAuthConsumerKey(consumerKey)
	         .setOAuthConsumerSecret(consumerSecret)
	         .setOAuthAccessToken(accessToken)
	         .setOAuthAccessTokenSecret(accessTokenSecret);

	      TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	      StatusListener listener = new StatusListener() {
	        
//	         @Override
	         public void onStatus(Status status) {      
	            queue.offer(status);
//	            System.out.println(status.getUser().getName() + " : " + status.getText());

	            // System.out.println("@" + status.getUser().getScreenName() 
	            //   + " - " + status.getText());
	            // System.out.println("@" + status.getUser().getScreen-Name());

	            /*for(URLEntity urle : status.getURLEntities()) {
	               System.out.println(urle.getDisplayURL());
	            }*/

	            /*for(HashtagEntity hashtage : status.getHashtagEntities()) {
	               System.out.println(hashtage.getText());
	            }*/
	         }
	         
//	         @Override
	         public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	            // System.out.println("Got a status deletion notice id:" 
	            //   + statusDeletionNotice.getStatusId());
	         }
	         
//	         @Override
	         public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	            // System.out.println("Got track limitation notice:" + 
	            //   num-berOfLimitedStatuses);
	         }

//	         @Override
	         public void onScrubGeo(long userId, long upToStatusId) {
	            // System.out.println("Got scrub_geo event userId:" + userId + 
	            // "upToStatusId:" + upToStatusId);
	         }      
	         
//	         @Override
	         public void onStallWarning(StallWarning warning) {
	            // System.out.println("Got stall warning:" + warning);
	         }
	         
//	         @Override
	         public void onException(Exception ex) {
	            ex.printStackTrace();
	         }
	      };
	      twitterStream.addListener(listener);
	      
	      FilterQuery query = new FilterQuery().language("en");//track(topicNameArr);
	      twitterStream.sample("en");//filter(query);

	      Thread.sleep(5000);
	      
	      //Add Kafka producer config settings
	      Properties props = new Properties();
	      props.put("bootstrap.servers", "localhost:9092");
	      props.put("acks", "all");
	      props.put("retries", 0);
	      props.put("batch.size", 16384);
	      props.put("linger.ms", 1);
	      props.put("buffer.memory", 33554432);
	      
	      props.put("key.serializer", 
	         "org.apache.kafka.common.serialization.StringSerializer");
	      props.put("value.serializer", 
	         "org.apache.kafka.common.serialization.StringSerializer");
	      
	      Producer<String, String> producer = new KafkaProducer<String, String>(props);
	      int i = 0;
	      int j = 0;
	      
	      long t= System.currentTimeMillis();
	      long end = t+600000;
	      while(System.currentTimeMillis() < end) {
//	      while(i < 1000000) {
	         Status status = queue.poll();
	         
	         if (status == null) {
	            i++;
//	            System.out.println("Content is null");
	         }
	         else {
//	        	System.out.println("Text: " + status.getText());
	            for(HashtagEntity hashtag : status.getHashtagEntities()) {
//	               System.out.println("Hashtag: " + hashtag.getText());
	            }
	            Record record = new Record(status);
	            String recordJson = record.toString();
	            if(record.getLocation() != null)
	            	System.out.println("Location found in " + recordJson);
	            if(record.getPlace() != null)
	            	System.out.println("Place found in " + recordJson);
	            
//	            System.out.println("Sending " + recordJson);
//	            System.out.println("Sending " + status.getText());
	            producer.send(new ProducerRecord<String, String>(
		                  topicName, Integer.toString(j++), recordJson));
	         }
	      }
	      Thread.sleep(60000);
	      producer.close();
	      Thread.sleep(5000);
	      twitterStream.shutdown();
	      System.out.println("Finished after " + i + " null statuses");
	      System.out.println(j + " Tweets overall processed");
	   }
	
	//TODO: Make private
	public String[] getTwitterConfigs() throws FileNotFoundException, IOException {
		Properties props = new Properties();
        props.load(new FileReader(TWITTER_CONFIG_FILE_PATH));

        String consumerKey = props.getProperty("consumerKey");
        String consumerSecret = props.getProperty("consumerSecret");
        String accessToken = props.getProperty("accessToken");
        String accessTokenSecret = props.getProperty("accessTokenSecret");

        String[] result = { consumerKey, consumerSecret, accessToken, accessTokenSecret };
        return result;
	}

}
