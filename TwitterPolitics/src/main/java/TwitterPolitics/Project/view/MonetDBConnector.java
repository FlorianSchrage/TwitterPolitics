/**
 * 
 */
package TwitterPolitics.Project.view;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Steffen Terheiden
 *
 */
public class MonetDBConnector {

	private final static String TABLE_NAME = "topics";

	public static void main(String[] args) {
		List<TopicRecord> topics = new ArrayList<>();
		topics.add(new TopicRecord(1243251L, "Germany", "Medium", "Trump, MAGA, METOO", 12355));
		topics.add(new TopicRecord(1243251L, "Germany", "Medium", "Trump, MAGA, sadfMETOO", 12355));
		insertTopics(topics);
	}

	public static void insertTopics(List<TopicRecord> topics) {
		try {
			PreparedStatement statement = getConnection().prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES(?, ?, ?, ?, ?)");

			for (TopicRecord record : topics) {
				statement.setTimestamp(1, record.getTimeStamp()); // some random time
				statement.setString(2, record.getRegion());
				statement.setString(3, record.getFollowerGroup());
				statement.setString(4, record.getTopic());
				statement.setInt(5, record.getCount());
				statement.addBatch();

			}
			statement.executeBatch();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection() {
		try {
			return DriverManager.getConnection("jdbc:monetdb://localhost:50000/demo?so_timeout=10000", "monetdb", "monetdb");
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static class TopicRecord {
		/**
		 * @param timeStamp
		 * @param region
		 * @param topic
		 * @param count
		 */
		public TopicRecord(long timeStamp, String region, String followerGroup, String topic, int count) {
			super();
			this.timeStamp = timeStamp;
			this.region = region;
			this.followerGroup = followerGroup;
			this.topic = topic;
			this.count = count;
		}

		/**
		 * @return the timeStamp
		 */
		public Timestamp getTimeStamp() {
			return new Timestamp(timeStamp);
		}

		/**
		 * @param timeStamp
		 *            the timeStamp to set
		 */
		public void setTimeStamp(long timeStamp) {
			this.timeStamp = timeStamp;
		}

		/**
		 * @return the region
		 */
		public String getRegion() {
			return region;
		}

		/**
		 * @param region
		 *            the region to set
		 */
		public void setRegion(String region) {
			this.region = region;
		}

		/**
		 * @return the topic
		 */
		public String getTopic() {
			return topic;
		}

		/**
		 * @param topic
		 *            the topic to set
		 */
		public void setTopic(String topic) {
			this.topic = topic;
		}

		/**
		 * @return the count
		 */
		public int getCount() {
			return count;
		}

		/**
		 * @param count
		 *            the count to set
		 */
		public void setCount(int count) {
			this.count = count;
		}

		public String getFollowerGroup() {
			return followerGroup;
		}

		public void setFollowerGroup(String followerGroup) {
			this.followerGroup = followerGroup;
		}

		long timeStamp;
		String region, followerGroup, topic;
		int count;

	}
}
