package jdbcpgbackup;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

public class Test {

	public static void main(String[] args) {
		final String jdbcUrl = args[0];
		final int num_records = Integer.parseInt(args[1]);
		Connection con = null;
		try {
			{
				con = DriverManager.getConnection(jdbcUrl);
				con.setAutoCommit(false);
				Statement stmt = con.createStatement();
				try {
					stmt.executeUpdate("DROP SCHEMA test CASCADE");
				} catch (SQLException e) {
					e.printStackTrace();
				}
				con.commit();
				stmt.executeUpdate("CREATE SCHEMA test");
				con.commit();
				stmt.executeUpdate("CREATE TABLE test.t (a serial, b int, c int, d varchar)");
				stmt.executeUpdate("CREATE INDEX ON test.t (b)");
				for (int i = 0; i < num_records; i++) {
					stmt.executeUpdate("INSERT INTO test.t (b, c, d) VALUES (" + i + ", 1, 'test')");
				}
				con.commit();
				try {
					stmt.executeUpdate("DROP SCHEMA testrestore CASCADE");
				} catch (SQLException e) {
					e.printStackTrace();
				}
				con.commit();
				stmt.executeUpdate("CREATE SCHEMA testrestore");
				con.commit();
				con.close();
			}
			System.out.println("done creating test table");
			Thread inserter = new Thread() {
				public void run() {
					Connection con = null;
					try {
						con = DriverManager.getConnection(jdbcUrl);
						con.setAutoCommit(false);
						System.out.println("starting inserter");
						PreparedStatement stmt = con.prepareStatement("INSERT INTO test.t (b, c, d) VALUES (?, 2, 'inserted')");
						for (int i = num_records; i < 2 * num_records; i++) {
							stmt.setInt(1, i);
							stmt.executeUpdate();
							con.commit();
							if (this.isInterrupted()) break;
						}
						stmt.close();
						System.out.println("inserter done");
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						try {
							if (con != null) con.close();
						} catch (SQLException ignore) {}
					}
				}
			};
			Thread deleter = new Thread() {
				public void run() {
					Connection con = null;
					try {
						con = DriverManager.getConnection(jdbcUrl);
						con.setAutoCommit(false);
						PreparedStatement stmt = con.prepareStatement("DELETE FROM test.t WHERE b = ?");
						Random random = new Random();
						System.out.println("starting deleter");
						for (int i = 0; i < num_records / 4; i++) {
							int r = random.nextInt(num_records);
							stmt.setInt(1, r);
							stmt.executeUpdate();
							con.commit();
							if (this.isInterrupted()) break;
						}
						stmt.close();
						System.out.println("deleter done");
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						try {
							if (con != null) con.close();
						} catch (SQLException ignore) {}
					}
				}
			};
			Thread updater = new Thread() {
				public void run() {
					Connection con = null;
					try {
						con = DriverManager.getConnection(jdbcUrl);
						con.setAutoCommit(false);
						PreparedStatement stmt = con.prepareStatement("UPDATE test.t SET c = 3, d = 'updated' WHERE b = ?");
						Random random = new Random();
						System.out.println("starting updater");
						for (int i = 0; i < num_records / 4; i++) {
							int r = random.nextInt(num_records);
							stmt.setInt(1, r);
							stmt.executeUpdate();
							con.commit();
							if (this.isInterrupted()) break;
						}
						stmt.close();
						System.out.println("updater done");
					} catch (SQLException e) {
						e.printStackTrace();
					} finally {
						try {
							if (con != null) con.close();
						} catch (SQLException ignore) {}
					}
				}
			};

			inserter.setDaemon(true);
			deleter.setDaemon(true);
			updater.setDaemon(true);
			{
				con = DriverManager.getConnection(jdbcUrl);
				con.setReadOnly(true);
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				System.out.println("starting dump");
				Statement stmt = con.createStatement();
				stmt.executeQuery("SELECT count(*) from test.t");
				stmt.close();

				inserter.start();
				deleter.start();
				updater.start();

				Thread.sleep(10000);

				ZipBackup backup = new ZipBackup("ttest.zip", jdbcUrl);
				backup.dump(Arrays.asList("test"), DataFilter.ALL_DATA, con);
				System.out.println("dump done");
				con.close();
			}			
			inserter.interrupt();
			deleter.interrupt();
			updater.interrupt();
			inserter.join();
			deleter.join();
			updater.join();

			System.out.println("starting restore");
			ZipBackup restore = new ZipBackup("ttest.zip", jdbcUrl);
			restore.restoreSchemaTo("test", "testrestore");
			System.out.println("end restore");
			{
				con = DriverManager.getConnection(jdbcUrl);
				Statement stmt = con.createStatement();
				String[] conditions = new String[]{
						"b < " + num_records,
						"b >= " + num_records,
						"c = 2",
						"c = 3",
						"d = 'inserted'",
						"d = 'updated'"
				};
				for (String condition : conditions) {
					String sql = "SELECT count(*) FROM test.t WHERE " + condition;
					ResultSet rs = stmt.executeQuery(sql);
					rs.next();
					System.out.println(sql + ": " + rs.getLong(1));
					rs.close();
					sql = "SELECT count(*) FROM testrestore.t WHERE " + condition;
					rs = stmt.executeQuery(sql);
					rs.next();
					System.out.println(sql + ": " + rs.getLong(1));
					rs.close();
				}
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (con != null) con.close();
			} catch (SQLException ignore) {}
		}
		System.exit(0);
	}
}
