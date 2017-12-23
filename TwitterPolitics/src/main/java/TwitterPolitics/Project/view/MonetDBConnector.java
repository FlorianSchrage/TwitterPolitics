/**
 * 
 */
package TwitterPolitics.Project.view;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Steffen Terheiden
 *
 */
public class MonetDBConnector {

	public static void main(String[] args) {
		try {
			System.out.println(getConnection().getCatalog());
			Statement st = getConnection().createStatement();
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
}
