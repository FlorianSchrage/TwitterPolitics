/**
 * 
 */
package TwitterPolitics.Project.streamIngestion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.Gson;

import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;

/**
 * @author Steffen Terheiden
 *
 */
public class Record {
	String text;
	List<String> hashtagList = new ArrayList<>();
	Date creationDate = new Date();
	String language;
	// twitter4j class, used as simple holder for the pair(latitude, longitude)
	GeoLocation location;
	// TODO: change to more general holder, since it is twitter specific
	Place place;

	public Record(Status tweet) {
		text = tweet.getText();
		hashtagList = Arrays.stream(tweet.getHashtagEntities()).map(h -> h.getText()).collect(Collectors.toList());
		creationDate = tweet.getCreatedAt();
		language = tweet.getLang();
//		location = tweet.getGeoLocation();
//		place = tweet.getPlace();

	}

	public static Record getByJsonString(String jsonRecord) {
		return new Gson().fromJson(jsonRecord, Record.class);
	}

	@Override
	public String toString() {
		return new Gson().toJson(this).toString();
	}

	/**
	 * @return the text
	 */
	public String getText() {
		return text;
	}

	/**
	 * @return the hashtagList
	 */
	public List<String> getHashtagList() {
		return hashtagList;
	}

	/**
	 * @return the creationDate
	 */
	public Date getCreationDate() {
		return creationDate;
	}

	/**
	 * @return the language
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * @return the location
	 */
	public GeoLocation getLocation() {
		return location;
	}

	/**
	 * @return the place
	 */
	public Place getPlace() {
		return place;
	}

}
