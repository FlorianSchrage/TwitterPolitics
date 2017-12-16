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
import com.google.gson.JsonSyntaxException;

import twitter4j.GeoLocation;
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
	Location location;
	// TODO: change to more general holder, since it is twitter specific
	Place place;

	public Record(Status tweet) {
		text = tweet.getText();
		hashtagList = Arrays.stream(tweet.getHashtagEntities()).map(h -> h.getText()).collect(Collectors.toList());
		creationDate = tweet.getCreatedAt();
		language = tweet.getLang();
		location = Location.getLocation(tweet.getGeoLocation());
		twitter4j.Place p = tweet.getPlace();
		if (p != null) {

			place = new Place(p.getName(), p.getStreetAddress(), p.getCountryCode(), p.getId(), p.getCountry(), p.getPlaceType(), p.getURL(), p.getFullName(),
					p.getBoundingBoxType(), Location.getLocations(p.getBoundingBoxCoordinates()), p.getGeometryType(),
					Location.getLocations(p.getGeometryCoordinates()));
		}
	}

	public static Record getByJsonString(String jsonRecord) {
		try {
			return new Gson().fromJson(jsonRecord, Record.class);
		} catch (JsonSyntaxException e) {
			System.out.println(e);
			return null;
		}
	}

	@Override
	public String toString() {
		return new Gson().toJson(this).toString();
	}

	public static class Place {
		public Place() {
			super();
		}

		/**
		 * @param name
		 * @param streetAddress
		 * @param countryCode
		 * @param id
		 * @param country
		 * @param placeType
		 * @param uRL
		 * @param fullName
		 * @param boundingBoxType
		 * @param boundingBoxCoordinates
		 * @param geometryType
		 * @param geometryCoordinates
		 * @param containedWithIn
		 */
		public Place(String name, String streetAddress, String countryCode, String id, String country, String placeType, String uRL, String fullName,
				String boundingBoxType, Location[][] boundingBoxCoordinates, String geometryType, Location[][] geometryCoordinates) {
			super();
			this.name = name;
			this.streetAddress = streetAddress;
			this.countryCode = countryCode;
			this.id = id;
			this.country = country;
			this.placeType = placeType;
			this.uRL = uRL;
			this.fullName = fullName;
			this.boundingBoxType = boundingBoxType;
			this.boundingBoxCoordinates = boundingBoxCoordinates;
			this.geometryType = geometryType;
			this.geometryCoordinates = geometryCoordinates;
		}

		String name;
		String streetAddress;
		String countryCode;
		String id;
		String country;
		String placeType;
		String uRL;
		String fullName;
		String boundingBoxType;
		Location[][] boundingBoxCoordinates;
		String geometryType;
		Location[][] geometryCoordinates;

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name
		 *            the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the streetAddress
		 */
		public String getStreetAddress() {
			return streetAddress;
		}

		/**
		 * @param streetAddress
		 *            the streetAddress to set
		 */
		public void setStreetAddress(String streetAddress) {
			this.streetAddress = streetAddress;
		}

		/**
		 * @return the countryCode
		 */
		public String getCountryCode() {
			return countryCode;
		}

		/**
		 * @param countryCode
		 *            the countryCode to set
		 */
		public void setCountryCode(String countryCode) {
			this.countryCode = countryCode;
		}

		/**
		 * @return the id
		 */
		public String getId() {
			return id;
		}

		/**
		 * @param id
		 *            the id to set
		 */
		public void setId(String id) {
			this.id = id;
		}

		/**
		 * @return the country
		 */
		public String getCountry() {
			return country;
		}

		/**
		 * @param country
		 *            the country to set
		 */
		public void setCountry(String country) {
			this.country = country;
		}

		/**
		 * @return the placeType
		 */
		public String getPlaceType() {
			return placeType;
		}

		/**
		 * @param placeType
		 *            the placeType to set
		 */
		public void setPlaceType(String placeType) {
			this.placeType = placeType;
		}

		/**
		 * @return the uRL
		 */
		public String getuRL() {
			return uRL;
		}

		/**
		 * @param uRL
		 *            the uRL to set
		 */
		public void setuRL(String uRL) {
			this.uRL = uRL;
		}

		/**
		 * @return the fullName
		 */
		public String getFullName() {
			return fullName;
		}

		/**
		 * @param fullName
		 *            the fullName to set
		 */
		public void setFullName(String fullName) {
			this.fullName = fullName;
		}

		/**
		 * @return the boundingBoxType
		 */
		public String getBoundingBoxType() {
			return boundingBoxType;
		}

		/**
		 * @param boundingBoxType
		 *            the boundingBoxType to set
		 */
		public void setBoundingBoxType(String boundingBoxType) {
			this.boundingBoxType = boundingBoxType;
		}

		/**
		 * @return the boundingBoxCoordinates
		 */
		public Location[][] getBoundingBoxCoordinates() {
			return boundingBoxCoordinates;
		}

		/**
		 * @param boundingBoxCoordinates
		 *            the boundingBoxCoordinates to set
		 */
		public void setBoundingBoxCoordinates(Location[][] boundingBoxCoordinates) {
			this.boundingBoxCoordinates = boundingBoxCoordinates;
		}

		/**
		 * @return the geometryType
		 */
		public String getGeometryType() {
			return geometryType;
		}

		/**
		 * @param geometryType
		 *            the geometryType to set
		 */
		public void setGeometryType(String geometryType) {
			this.geometryType = geometryType;
		}

		/**
		 * @return the geometryCoordinates
		 */
		public Location[][] getGeometryCoordinates() {
			return geometryCoordinates;
		}

		/**
		 * @param geometryCoordinates
		 *            the geometryCoordinates to set
		 */
		public void setGeometryCoordinates(Location[][] geometryCoordinates) {
			this.geometryCoordinates = geometryCoordinates;
		}

	}

	public static class Location {

		public static Location getLocation(GeoLocation geoLocation) {
			if (geoLocation == null)
				return null;
			return new Location(geoLocation.getLatitude(), geoLocation.getLongitude());
		}

		public static Location[][] getLocations(GeoLocation[][] geoLocations) {
			if (geoLocations == null)
				return null;
			Location[][] locations = new Location[geoLocations.length][];
			for (int i = 0; i < geoLocations.length; i++) {
				GeoLocation[] geoLocations2 = geoLocations[i];
				locations[i] = new Location[geoLocations2.length];
				for (int j = 0; j < geoLocations2.length; j++) {
					locations[i][j] = getLocation(geoLocations2[j]);

				}
			}
			return locations;
		}

		public Location() {
			super();
		}

		/**
		 * @param latitude
		 * @param longitude
		 */
		public Location(double latitude, double longitude) {
			super();
			this.latitude = latitude;
			this.longitude = longitude;
		}

		private double latitude, longitude;

		/**
		 * @return the latitude
		 */
		public double getLatitude() {
			return latitude;
		}

		/**
		 * @param latitude
		 *            the latitude to set
		 */
		public void setLatitude(double latitude) {
			this.latitude = latitude;
		}

		/**
		 * @return the longitude
		 */
		public double getLongitude() {
			return longitude;
		}

		/**
		 * @param longitude
		 *            the longitude to set
		 */
		public void setLongitude(double longitude) {
			this.longitude = longitude;
		}

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
	public Location getLocation() {
		return location;
	}

	/**
	 * @return the place
	 */
	public Place getPlace() {
		return place;
	}

}
