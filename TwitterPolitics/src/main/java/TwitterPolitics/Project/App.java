package TwitterPolitics.Project;

import java.io.FileNotFoundException;
import java.io.IOException;

import TwitterPolitics.Project.streamIngestion.KafkaTwitterIngestion;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		System.out.println("Hello World!123");
		System.out.println("Ich war hier");
		
		KafkaTwitterIngestion kafkaTwitter = new KafkaTwitterIngestion();
		
		String[] twitterCreds = new String[0];
		try {
			twitterCreds = kafkaTwitter.getTwitterConfigs();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < twitterCreds.length; i++) {
			System.out.println(twitterCreds[i]);
		}
		
		try {
			kafkaTwitter.firstDraft();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
