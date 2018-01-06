package TwitterPolitics.Project.streamIngestion;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import TwitterPolitics.Project.streamProcessing.StreamProcessor;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class KafkaTwitterIngestion {
	private static final String TWITTER_CONFIG_FILE_PATH = "src/main/resources/twitter_configuration.txt";
	private static final int QUEUE_CAPACITY = 1000;

	private static final int DEFAULT_RUNTIME_MINS = 3;

	private static List<Integer> all = new ArrayList<>();

	private LinkedBlockingQueue<Status> queue;

	public void ingestTwitterStream(int runtime) throws Exception {
		queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

		String[] twitterConfigs = getTwitterConfigs();

		String consumerKey = twitterConfigs[0];
		String consumerSecret = twitterConfigs[1];
		String accessToken = twitterConfigs[2];
		String accessTokenSecret = twitterConfigs[3];
		String topicName = StreamProcessor.TOPIC;
		// String[] topicNameArr = { StreamProcessor.TOPIC };
		// String[] arguments = args.clone();
		// String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {

			// @Override
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
				// System.out.println(status.getUser().getName() + " : " + status.getText());

				// System.out.println("@" + status.getUser().getScreenName()
				// + " - " + status.getText());
				// System.out.println("@" + status.getUser().getScreen-Name());

				/*
				 * for(URLEntity urle : status.getURLEntities()) {
				 * System.out.println(urle.getDisplayURL());
				 * }
				 */

				/*
				 * for(HashtagEntity hashtage : status.getHashtagEntities()) {
				 * System.out.println(hashtage.getText());
				 * }
				 */
			}

			// @Override
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				// System.out.println("Got a status deletion notice id:"
				// + statusDeletionNotice.getStatusId());
			}

			// @Override
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// System.out.println("Got track limitation notice:" +
				// num-berOfLimitedStatuses);
			}

			// @Override
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				// System.out.println("Got scrub_geo event userId:" + userId +
				// "upToStatusId:" + upToStatusId);
			}

			// @Override
			@Override
			public void onStallWarning(StallWarning warning) {
				// System.out.println("Got stall warning:" + warning);
			}

			// @Override
			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);

		String[] testarr = new String[400];
		List<String> testlist = StreamProcessor.getInitialHashtags();
		System.out.println("Lenght: " + testlist.size());
		for (int i = 0; i < testarr.length; i++) {
			testarr[i] = "#" + testlist.get(i);
		}

		FilterQuery query = new FilterQuery().track(testarr).language("en");// StreamProcessor.getInitialHashtags().toArray(new String[0]));
		twitterStream.filter(query);// sample("en");

		Thread.sleep(5000);

		// Add Kafka producer config settings
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

		Producer<String, String> producer = new KafkaProducer<>(props);
		long i = 0;
		long j = 0;

		long t = System.currentTimeMillis();
		long end = t + 60000 * runtime;
		// end = t + 10000;
		while (System.currentTimeMillis() < end) {
			Status status = queue.poll();

			if (status == null) {
				// System.out.println("Content is null");
			} else {
				// System.out.println("Text: " + status.getText());
				// for(HashtagEntity hashtag : status.getHashtagEntities()) {
				// System.out.println("Hashtag: " + hashtag.getText());
				// }
				if (status.getHashtagEntities().length > 0)
					i++;

				all.add(status.getUser().getFollowersCount());
				Record record = new Record(status);
				String recordJson = record.toString();
				// if(record.getLocation() != null)
				// System.out.println("Location found in " + recordJson);
				// if(record.getPlace() != null)
				// System.out.println("Place found in " + recordJson);

				producer.send(new ProducerRecord<>(
						topicName, Long.toString(j++), recordJson));
			}
		}
		Thread.sleep(60000);
		producer.close();
		Thread.sleep(5000);
		twitterStream.shutdown();
		System.out.println("Finished");
		System.out.println(j + " Tweets overall processed");
		System.out.println(i + " Tweets with hashtags");
		System.out.println((j - i) + " Tweets without hashtags");

		// Collections.sort(all);
		// int small = all.size() / 3;
		// int medium = all.size() * 2 / 3;
		// int k = 1;
		// ArrayList<Integer> smallList = new ArrayList<>();
		// ArrayList<Integer> mediumList = new ArrayList<>();
		// ArrayList<Integer> bigList = new ArrayList<>();
		//
		// for (Iterator<Integer> iterator = all.iterator(); iterator.hasNext();) {
		// Integer value = iterator.next();
		// if(k >= medium) {
		// bigList.add(value);
		// }
		// else if(k >= small) {
		// mediumList.add(value);
		// }
		// else {
		// smallList.add(value);
		// }
		//
		// k++;
		// }
		//
		// System.out.println("Small avg: " + smallList.stream().mapToInt(val -> val).average().getAsDouble());
		// System.out.println("Small max: " + smallList.get(smallList.size() - 1));
		// System.out.println("Small min: " + smallList.get(0));
		// System.out.println("Medium avg: " + mediumList.stream().mapToInt(val -> val).average().getAsDouble());
		// System.out.println("Medium max: " + mediumList.get(mediumList.size() - 1));
		// System.out.println("Medium min: " + mediumList.get(0));
		// System.out.println("Big avg: " + bigList.stream().mapToInt(val -> val).average().getAsDouble());
		// System.out.println("Big max: " + bigList.get(bigList.size() - 1));
		// System.out.println("Big min: " + bigList.get(0));
	}

	private String[] getTwitterConfigs() throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.load(new FileReader(TWITTER_CONFIG_FILE_PATH));

		String consumerKey = props.getProperty("consumerKey");
		String consumerSecret = props.getProperty("consumerSecret");
		String accessToken = props.getProperty("accessToken");
		String accessTokenSecret = props.getProperty("accessTokenSecret");

		String[] result = { consumerKey, consumerSecret, accessToken, accessTokenSecret };
		return result;
	}

	public static void main(String args[]) {
		KafkaTwitterIngestion kafkaTwitter = new KafkaTwitterIngestion();
		int runtime = DEFAULT_RUNTIME_MINS;

		if (args.length > 0) {
			try {
				runtime = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
			}
		}

		try {
			kafkaTwitter.ingestTwitterStream(runtime);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
