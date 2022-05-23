/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public final class ZipBackup {

	private static final String zipRoot = "pg_backup/";
	public static final int DEFAULT_BATCH_SIZE = 10000;

	private final String jdbcUrl;
	private final File file;

	private DBOFactory<Schema> schemaFactory = new Schema.SchemaFactory();
	private DBOFactory<View> viewFactory = new View.ViewFactory();
	private DBOFactory<Table> tableFactory = new Table.TableFactory();
	private DBOFactory<Sequence> sequenceFactory = new Sequence.SequenceFactory();
	private DBOFactory<Index> indexFactory = new Index.IndexFactory();
	private DBOFactory<Constraint> constraintFactory = new Constraint.ConstraintFactory();

	public ZipBackup(File file, String jdbcUrl) {
		this.file = file;
		this.jdbcUrl = jdbcUrl;
	}

	public ZipBackup(Map<String,String> params) {
		this(params.get("filename") == null ? null : new File(params.get("filename")),
				buildJdbcUrl(params));
	}

	public void dumpAll(DataFilter dataFilter) {
		dumpAll(dataFilter, DEFAULT_BATCH_SIZE);
	}

	public void dumpAll(DataFilter dataFilter, int batchSize) {
		debug("starting full dump at " + new Date());
		Schema.CachingSchemaFactory cachingSchemaFactory = new Schema.CachingSchemaFactory();
		schemaFactory = cachingSchemaFactory;
		Connection con = null;
		ZipOutputStream zos = null;
		try {
			zos = getZipOutputStream();
			con = DriverManager.getConnection(jdbcUrl);
			con.setReadOnly(true);
			con.setAutoCommit(true);
			timerStart("schemas");
			Collection<Schema> schemas = cachingSchemaFactory.getDbBackupObjects(con, null);
			setTotalCount(schemas.size());
			dumpSchemasSql(schemas, dataFilter, con, zos);
			debug(schemas.size() + " schemas to be dumped");
			timerEnd("schemas");
			debug("begin dumping schemas");
			Collection<Schema> batch;
			while (! (batch = cachingSchemaFactory.nextBatch(con, batchSize)).isEmpty()) {
				viewFactory = new View.CachingViewFactory(cachingSchemaFactory);
				tableFactory = new Table.CachingTableFactory(cachingSchemaFactory);
				sequenceFactory = new Sequence.CachingSequenceFactory(cachingSchemaFactory);
				indexFactory = new Index.CachingIndexFactory(cachingSchemaFactory, (Table.CachingTableFactory)tableFactory);
				constraintFactory = new Constraint.CachingConstraintFactory(cachingSchemaFactory, (Table.CachingTableFactory)tableFactory);
				for (Schema schema : batch) {
					dump(schema, dataFilter, con, zos);
				}
				con.close();
				con = DriverManager.getConnection(jdbcUrl);
				con.setReadOnly(true);
				con.setAutoCommit(true);
			}
			printTimings();
		} catch (SQLException e) {
			throw new RuntimeException(e.getMessage(), e);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (con != null) con.close();
			} catch (SQLException ignore) {}
			try {
				if (zos != null) zos.close();
			} catch (IOException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		debug("finished full dump at " + new Date());
	}

	public void dump(Iterable<String> schemaNames, DataFilter dataFilter) {
		Connection con = null;
		try {
			con = DriverManager.getConnection(jdbcUrl);
			con.setReadOnly(true);
			con.setAutoCommit(false);
			con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			dump(schemaNames, dataFilter, con);
		} catch (SQLException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (con != null) con.close();
			} catch (SQLException ignore) {}
		}
	}

	public void dump(Iterable<String> schemaNames, DataFilter dataFilter, Connection con) {
		ZipOutputStream zos = null;
		try {
			zos = getZipOutputStream();
			timerStart("schemas");
			List<Schema> schemas = new ArrayList<Schema>();
			for (String schemaName : schemaNames) {
				Schema schema = schemaFactory.getDbBackupObject(con, schemaName, null);
				if (schema == null) 
					throw new RuntimeException("schema " + schemaName + " not found in database");
				schemas.add(schema);
			}
			setTotalCount(schemas.size());
			dumpSchemasSql(schemas, dataFilter, con, zos);
			timerEnd("schemas");
			for (Schema schema : schemas) {
				dump(schema, dataFilter, con, zos);
			}
			printTimings();
		} catch (SQLException e) {
			throw new RuntimeException(e.getMessage(), e);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (zos != null) zos.close();
			} catch (IOException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	private ZipOutputStream getZipOutputStream() throws IOException {
		if (file != null) {
			if (file.length() > 0) {
				throw new RuntimeException("destination file is not empty");
			}
			return new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
		} else {
			return new ZipOutputStream(System.out);
		}
	}

	private void dumpSchemasSql(Iterable<Schema> schemas, DataFilter dataFilter, Connection con, ZipOutputStream zos) {
		try {
			zos.putNextEntry(new ZipEntry(zipRoot));
			putSqlZipEntry(zos, zipRoot+"schemas.sql", schemas, dataFilter);
			zos.putNextEntry(new ZipEntry(zipRoot + "schemas/"));
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private void dump(Schema schema, DataFilter dataFilter, Connection con, ZipOutputStream zos) {
		try {
			String schemaRoot = zipRoot + "schemas/" + schema.getName() + "/";
			zos.putNextEntry(new ZipEntry(schemaRoot));

			timerStart("sequences");
			Iterable<Sequence> sequences = sequenceFactory.getDbBackupObjects(con, schema);
			putSqlZipEntry(zos, schemaRoot + "sequences.sql", sequences, dataFilter);
			timerEnd("sequences");

			Iterable<Table> tables = tableFactory.getDbBackupObjects(con, schema);
			putSqlZipEntry(zos, schemaRoot + "tables.sql", tables, dataFilter);

			timerStart("table data");
			zos.putNextEntry(new ZipEntry(schemaRoot + "tables/"));
			for (Table table : tables) {
				if (dataFilter.dumpData(schema.getName(), table.getName())) {
					zos.putNextEntry(new ZipEntry(schemaRoot + "tables/" + table.getName()));
					table.dump(con, zos);
				}
			}
			timerEnd("table data");

			timerStart("views");
			Iterable<View> views = viewFactory.getDbBackupObjects(con, schema);
			putSqlZipEntry(zos, schemaRoot + "views.sql", views, dataFilter);
			timerEnd("views");

			timerStart("indexes");
			Iterable<Index> indexes = indexFactory.getDbBackupObjects(con, schema);
			putSqlZipEntry(zos, schemaRoot + "indexes.sql", indexes, dataFilter);
			timerEnd("indexes");

			timerStart("constraints");
			Iterable<Constraint> constraints = constraintFactory.getDbBackupObjects(con, schema);
			putSqlZipEntry(zos, schemaRoot + "constraints.sql", constraints, dataFilter);
			timerEnd("constraints");

			processedSchema();

		} catch (SQLException e) {
			throw new RuntimeException("error dumping schema " + schema.getName(), e);
		} catch (IOException e) {
			throw new RuntimeException("error dumping schema " + schema.getName(), e);
		}
	}

	private void putSqlZipEntry(ZipOutputStream zos, String name,
			Iterable<? extends DbBackupObject> dbBackupObjects, DataFilter dataFilter) throws IOException {
		zos.putNextEntry(new ZipEntry(name));
		for (DbBackupObject o : dbBackupObjects) {
			zos.write(o.getSql(dataFilter).getBytes());
		}
	}


	public List<String> schemasInBackup() {
		ZipFile zipFile = null;
		try {
			zipFile = new ZipFile(file);
			return new ArrayList<String>(getSchemaTables(zipFile).keySet());
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (zipFile != null) zipFile.close();
			} catch (IOException ignore) {}
		}
	}

	public void restoreSchema(String schema) {
		restoreSchemaTo(schema, schema);
	}

	public void restoreSchemaTo(String schema, String toSchema) {
		Connection con = null;
		try {
			con = DriverManager.getConnection(jdbcUrl);
			con.setAutoCommit(false);
			restoreSchemaTo(schema, toSchema, con);
			con.commit();
		} catch (Exception e) {
			try {
				if (con != null) con.rollback();
			} catch (SQLException ignore) {}
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (con != null) con.close();
			} catch (SQLException ignore) {}
		}
	}

	public void restoreSchemaTo(String schema, String toSchema, Connection con) {
		ZipFile zipFile = null;
		try {
			zipFile = new ZipFile(file);
			restoreSchema(schema, toSchema, toSchema, zipFile, con);
			printTimings();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (zipFile != null) zipFile.close();
			} catch (IOException ignore) {}
		}
	}

	public void restoreAll() {
		debug("starting full restore at " + new Date());
		ZipFile zipFile = null;
		Connection con = null;
		try {
			con = DriverManager.getConnection(jdbcUrl);
			con.setAutoCommit(false);
			zipFile = new ZipFile(file);

			timerStart("schemas");
			restoreSchemasSql(zipFile, con);
			List<String> schemas = schemasInBackup();
			setTotalCount(schemas.size());
			timerEnd("schemas");

			int count = 0;
			for (String schemaName : schemas) {
				restoreSchema(schemaName, schemaName, schemaName, zipFile, con);
				if (++count%100 == 1) con.commit(); // commit every 100 schemas
			}

			con.commit();
			printTimings();
		} catch (Exception e) {
			try {
				if (con != null) con.rollback();
			} catch (SQLException ignore) {}
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (con != null) con.close();
			} catch (SQLException ignore) {}
			try {
				if (zipFile != null) zipFile.close();
			} catch (IOException ignore) {}
		}
		debug("finished full restore at " + new Date());
	}

	private void restoreSchema(String fromSchemaName, String toSchemaName, String toOwner, ZipFile zipFile, Connection con) {
		try {
			timerStart("schemas");
			boolean isNewSchema = !toSchemaName.equals(fromSchemaName);
			Schema toSchema = schemaFactory.getDbBackupObject(con, toSchemaName, null);
			if (toSchema == null) 
				toSchema = Schema.createSchema(con, toSchemaName, toOwner, schemaFactory);
			else
				toOwner = toSchema.getOwner(); // preserve existing owner
			setRole(con, toOwner);
			setSearchPath(con, toSchema);
			timerEnd("schemas");

			String schemaRoot = zipRoot + "schemas/" + fromSchemaName + "/";

			timerStart("sequences");
			ZipEntry sequencesSql = zipFile.getEntry(schemaRoot + "sequences.sql");
			execSqlZipEntry(zipFile, con, sequencesSql, isNewSchema);
			timerEnd("sequences");

			timerStart("tables");
			ZipEntry tablesSql = zipFile.getEntry(schemaRoot + "tables.sql");
			execSqlZipEntry(zipFile, con, tablesSql, isNewSchema);
			timerEnd("tables");

			timerStart("table data");
			Set<ZipEntry> tableEntries = getSchemaTables(zipFile).get(fromSchemaName);
			for (ZipEntry tableEntry : tableEntries) {
				String tableName = parseTable(tableEntry.getName());
				Table table = tableFactory.getDbBackupObject(con, tableName, toSchema);
				if (!table.getOwner().equals(toOwner) && !isNewSchema) {
					setRole(con, table.getOwner());
				}
				table.restore(zipFile.getInputStream(tableEntry), con);
				if (!table.getOwner().equals(toOwner) && !isNewSchema) {
					setRole(con, toOwner);
				}
			}
			timerEnd("table data");

			timerStart("views");
			ZipEntry viewsSql = zipFile.getEntry(schemaRoot + "views.sql");
			execSqlZipEntry(zipFile, con, viewsSql, isNewSchema);
			timerEnd("views");

			timerStart("indexes");
			ZipEntry indexesSql = zipFile.getEntry(schemaRoot + "indexes.sql");
			execSqlZipEntry(zipFile, con, indexesSql, isNewSchema);
			timerEnd("indexes");

			timerStart("constraints");
			ZipEntry constraintsSql = zipFile.getEntry(schemaRoot + "constraints.sql");
			execSqlZipEntry(zipFile, con, constraintsSql, isNewSchema);
			timerEnd("constraints");

			resetSearchPath(con);
			resetRole(con);
			processedSchema();
		} catch (Exception e) {
			throw new RuntimeException(
					"error restoring " + fromSchemaName + 
					" to " + toSchemaName, e
					);
		}
	}

	private void restoreSchemasSql(ZipFile zipFile, Connection con) {
		try {
			ZipEntry schemasSql = zipFile.getEntry(zipRoot + "schemas.sql");
			if (schemasSql!=null) execSqlZipEntry(zipFile, con, schemasSql, false);
		} catch (SQLException e) {
			throw new RuntimeException(e.getMessage(), e);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private void execSqlZipEntry(ZipFile zipFile, Connection con, ZipEntry zipEntry, boolean isNewSchema) throws IOException, SQLException {	
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(zipFile.getInputStream(zipEntry)));
			for (String sql = reader.readLine(); sql != null; sql = reader.readLine()) {
				if (isNewSchema) { // skip any role and ownership changes if restoring to new schema
					if (sql.startsWith("SET ROLE ") || (sql.startsWith("ALTER ") && sql.contains(" OWNER TO "))) {
						continue;
					}
				}
				PreparedStatement stmt = null;
				try {
					stmt = con.prepareStatement(sql);
					if (sql.startsWith("SELECT ")) {
						stmt.executeQuery();
					} else {
						stmt.executeUpdate();
					}
				} catch (SQLException e) {
					throw new RuntimeException("error executing sql: " + sql, e);
				} finally {
					if (stmt != null) stmt.close();
				}
			}
		} finally {
			if (reader != null) reader.close();
		}
	}


	private Map<String,Set<ZipEntry>> schemaTables = null;

	private Map<String,Set<ZipEntry>> getSchemaTables(ZipFile zipFile) {
		if (schemaTables == null) {
			schemaTables = new HashMap<String,Set<ZipEntry>>();
			Enumeration<? extends ZipEntry> entries = zipFile.entries();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				String schema = parseSchema(entry.getName());
				if (schema == null) continue;
				Set<ZipEntry> tables = schemaTables.get(schema);
				if (tables == null) {
					tables = new HashSet<ZipEntry>();
					schemaTables.put(schema, tables);
				}
				if (isTable(entry.getName())) {
					tables.add(entry);
				}
			}
		}
		return schemaTables;
	}

	private void setRole(Connection con, String role) throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = con.prepareStatement("SET ROLE " + role);
			//stmt.setString(1, role);
			stmt.executeUpdate();
		} finally {
			if (stmt != null) stmt.close();
		}
	}

	private void setSearchPath(Connection con, Schema schema) throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = con.prepareStatement("SET SEARCH_PATH = " + schema.getName());
			//stmt.setString(1, schema.getName());
			stmt.executeUpdate();
		} finally {
			if (stmt != null) stmt.close();
		}
	}

	private void resetRole(Connection con) throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = con.prepareStatement("RESET ROLE");
			stmt.executeUpdate();
		} finally {
			if (stmt != null) stmt.close();
		}
	}

	private void resetSearchPath(Connection con) throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = con.prepareStatement("RESET SEARCH_PATH");
			stmt.executeUpdate();
		} finally {
			if (stmt != null) stmt.close();
		}
	}

	private static boolean isTable(String name) {
		int i = name.indexOf("/tables/");
		return i > -1 && i < name.length() - "/tables/".length();
	}

	private static String parseTable(String name) {
		int from = name.indexOf("/tables/") + "/tables/".length();
		return name.substring(from);
	}

	private static String parseSchema(String name) {
		int i = name.indexOf("/schemas/");
		if (i < 0) return null;
		int from = i + "/schemas/".length();
		int to = name.indexOf('/', from);
		if (to < 0) return null;
		return name.substring(from, to); 
	}



	private static class Timing {
		private final Map<String,Long> timerMap = new HashMap<String,Long>();
		private final long startTime = System.currentTimeMillis();
		private final PrintStream ps;
		private int totalCount = 1;
		private int processedCount;
		private long time = -1;

		private Timing(PrintStream ps) {
			this.ps = ps;
		}

		void start(String step) {
			if (time != -1) throw new RuntimeException();
			time = System.currentTimeMillis();
		}

		void end(String step) {
			if (time == -1) throw new RuntimeException();
			long thisTime = System.currentTimeMillis() - time;
			Long oldTotal = timerMap.get(step);
			if (oldTotal != null) thisTime += oldTotal.longValue();
			timerMap.put(step, new Long(thisTime));
			time = -1;
		}

		void processedSchema() {
			if (++processedCount % 100 == 1) print();
		}

		void print() {
			long totalTime = System.currentTimeMillis() - startTime;
			long remaining = totalTime;
			for (Map.Entry<String,Long> entry : timerMap.entrySet()) {
				long t = entry.getValue();
				remaining = remaining - t;
				ps.print(entry.getKey() + ":  \t");
				ps.println(t/1000 + " s \t" + t*100/totalTime + " %");
			}
			ps.println("others: \t" + remaining/1000 + " s \t" + remaining*100/totalTime + " %");
			ps.println("total time: \t" + totalTime/1000 + " s");
			ps.println("processed: \t" + processedCount + " out of " + totalCount +
					" schemas \t" + processedCount*100/totalCount + "%");
			ps.println();
		}

		void print(String msg) {
			ps.println(msg);
		}

	}

	static final ThreadLocal<Timing> timing = new ThreadLocal<Timing>() {
		@Override
		protected Timing initialValue() {
			return new Timing(null) {
				void start(String step) {}
				void end(String step) {}
				void processedSchema() {}
				void print() {}
				void print(String msg) {}
			};
		}
	};

	public static void setTimingOutput(PrintStream ps) {
		timing.set(new Timing(ps));
	}

	static void timerStart(String step) {
		timing.get().start(step);
	}

	static void timerEnd(String step) {
		timing.get().end(step);
	}

	static void setTotalCount(int n) {
		timing.get().totalCount = n;
	}

	static void processedSchema() {
		timing.get().processedSchema();
	}

	static void printTimings() {
		timing.get().print();
	}

	static void debug(String msg) {
		timing.get().print("at " + (System.currentTimeMillis() - timing.get().startTime)/1000 + " s:");
		timing.get().print(msg);
	}

	static String buildJdbcUrl(Map<String,String> params) {
		StringBuilder buf = new StringBuilder();
		buf.append("jdbc:postgresql://");
		String hostname = params.get("hostname");
		if (hostname == null) hostname = "localhost";
		buf.append(hostname);
		String port = params.get("port");
		if (port == null) port = "5432";
		buf.append(":").append(port);
		buf.append("/");
		String username = params.get("user");
		if (username == null) username = "postgres";
		String database = params.get("database");
		if (database == null) database = username;
		buf.append(database);
		buf.append("?user=");
		buf.append(username);
		String password = params.get("password");
		if (password != null) buf.append("&password=").append(password);
		else {
			File pgpass = new File(System.getProperty("user.home"), ".pgpass");
			if (pgpass.exists()) {
				BufferedReader r = null;
				try {
					r = new BufferedReader(new FileReader(pgpass));
					String line;
					while ((line = r.readLine()) != null) {
						if (line.startsWith("#")) continue;
						String[] entries = line.split(":");
						if (entries.length != 5) throw new RuntimeException(
								"unsupported pgpass file format, better specify password on command line");
						if (! (hostname.equals(entries[0]) || "*".equals(entries[0])) ) continue;
						if (! (port.equals(entries[1]) || "*".equals(entries[1])) ) continue;
						if (! (database.equals(entries[2]) || "*".equals(entries[2])) ) continue;
						if (! (username.equals(entries[3]) || "*".equals(entries[3])) ) continue;
						buf.append("&password=").append(entries[4]);
						break;
					}
				} catch (IOException e) {
					throw new RuntimeException("failed to read the pgpass file");
				} finally {
					try {
						if (r != null) r.close();
					} catch (IOException ignore) {}
				}
			}
		}
		return buf.toString();
	}

}
