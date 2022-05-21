/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

final class Schema extends DbBackupObject {

	static class SchemaFactory implements DBOFactory<Schema> {

		@Override
		public List<Schema> getDbBackupObjects(Connection con, Schema ignored) throws SQLException {
			List<Schema> schemas = new ArrayList<Schema>();
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT nspname, pg_get_userbyid(nspowner) AS owner, oid FROM pg_namespace " +
								"WHERE nspname NOT LIKE 'pg_%' " +
						"AND nspname <> 'information_schema'");
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					schemas.add(new Schema(rs.getString("nspname"), rs.getString("owner"), rs.getInt("oid")));
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return schemas;
		}

		@Override
		public Schema getDbBackupObject(Connection con, String schemaName, Schema ignored) throws SQLException {
			PreparedStatement stmt = null;
			Schema schema = null;
			try {
				stmt = con.prepareStatement(
						"SELECT pg_get_userbyid(nspowner) AS owner, oid FROM pg_namespace WHERE nspname = ?");
				stmt.setString(1, schemaName);
				ResultSet rs = stmt.executeQuery();
				if (rs.next())
					schema = new Schema(schemaName, rs.getString("owner"), rs.getInt("oid"));
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return schema;
		}

	}

	static class CachingSchemaFactory implements DBOFactory<Schema> { // does not extend CachingDBOFactory

		protected Map<String,Schema> map = null;
		private final Map<Integer,Schema> batch = new HashMap<Integer,Schema>();
		private Iterator<Schema> itr;

		@Override
		public Collection<Schema> getDbBackupObjects(Connection con, Schema ignored) throws SQLException {
			if (map == null) {
				loadMap(con);
			}
			return Collections.unmodifiableCollection(map.values());
		}

		@Override
		public Schema getDbBackupObject(Connection con, String name, Schema ignored) throws SQLException {
			if (map == null) {
				loadMap(con);
			}
			return map.get(name);
		}

		public Collection<Schema> nextBatch(Connection con, int batchSize) throws SQLException {
			if (itr == null) {
				itr = getDbBackupObjects(con, null).iterator();
			}
			batch.clear();
			while (itr.hasNext() && batch.size() < batchSize) {
				Schema schema = itr.next();
				batch.put(schema.getOid(), schema);
			}
			return Collections.unmodifiableCollection(batch.values());
		}

		public Schema getFromCurrentBatch(int oid) {
			return batch.get(oid);
		}

		private void loadMap(Connection con) throws SQLException {
			map = new HashMap<String,Schema>();
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT nspname AS schemaname, pg_get_userbyid(nspowner) AS owner, oid FROM pg_namespace " +
								"WHERE nspname NOT LIKE 'pg_%' " +
						"AND nspname <> 'information_schema'");
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					String schemaName = rs.getString("schemaname");
					map.put(schemaName, new Schema(rs.getString("schemaname"), rs.getString("owner"), rs.getInt("oid")));
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
		}

	}

	static Schema createSchema(Connection con, String schemaName, String owner, DBOFactory<Schema> schemaFactory) throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = con.prepareStatement(
					"CREATE SCHEMA " + schemaName + " AUTHORIZATION " + owner);
			stmt.executeUpdate();
		} finally {
			if (stmt != null) stmt.close();
		}
		return schemaFactory.getDbBackupObject(con, schemaName, null);
	}

	private final int oid;

	private Schema(String name, String owner, int oid) {
		super(name, null, owner);
		this.oid = oid;
	}

	@Override
	String getSql(DataFilter dataFilter) {
		return "CREATE SCHEMA " + name + " AUTHORIZATION " + owner + " ;\n";
	}

	@Override
	protected StringBuilder appendCreateSql(StringBuilder buf) {
		throw new UnsupportedOperationException();
	}

	@Override
	String getFullname() {
		return name;
	}

	int getOid() {
		return oid;
	}
}
