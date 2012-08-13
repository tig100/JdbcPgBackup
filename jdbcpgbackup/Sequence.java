/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class Sequence extends DbBackupObject {

	static class SequenceFactory implements DBOFactory<Sequence> {

		@Override
		public Iterable<Sequence> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
			List<Sequence> sequences = new ArrayList<Sequence>();
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT c.relname AS sequencename, pg_get_userbyid(c.relowner) AS owner FROM pg_class c " +
						"WHERE c.relkind='S' AND c.relnamespace = ?");
				stmt.setInt(1, schema.getOid());
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					Sequence sequence = getSequence(con, schema, rs.getString("sequencename"), rs.getString("owner"));
					sequences.add(sequence);
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return sequences;
		}

		@Override
		public Sequence getDbBackupObject(Connection con, String sequenceName, Schema schema) throws SQLException {
			PreparedStatement stmt = null;
			Sequence sequence = null;
			try {
				stmt = con.prepareStatement(
						"SELECT c.relname AS sequencename, pg_get_userbyid(c.relowner) AS owner FROM pg_class c " +
						"WHERE c.relkind='S' AND c.relnamespace = ? AND c.relname = ?");
				stmt.setInt(1, schema.getOid());
				stmt.setString(2, sequenceName);
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					sequence = getSequence(con, schema, rs.getString("sequencename"), rs.getString("owner"));
				} else {
					throw new RuntimeException("no such sequence " + sequenceName);
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return sequence;
		}

	}

	static class CachingSequenceFactory extends CachingDBOFactory<Sequence> {

		protected CachingSequenceFactory(Schema.CachingSchemaFactory schemaFactory) {
			super(schemaFactory);
		}

		@Override
		protected PreparedStatement getAllStatement(Connection con) throws SQLException {
			return con.prepareStatement(
					"SELECT c.relname AS sequencename, pg_get_userbyid(c.relowner) AS owner, " +
							"c.relnamespace AS schema_oid FROM pg_class c " +
					"WHERE c.relkind='S'");
		}

		@Override
		protected Sequence newDbBackupObject(Connection con, ResultSet rs, Schema schema) throws SQLException {
			return getSequence(con, schema, rs.getString("sequencename"), rs.getString("owner"));
		}


	}

	private static Sequence getSequence(Connection con, Schema schema, String sequenceName, String owner) throws SQLException {
		PreparedStatement stmt = null;
		Sequence sequence = null;
		try {
			stmt = con.prepareStatement(
					"SELECT * FROM " + schema.getName() + "." + sequenceName);
			ResultSet rs = stmt.executeQuery();
			if (rs.next())
				sequence = new Sequence(sequenceName, rs, schema, owner);
			else
				throw new RuntimeException("no such sequence: " + sequenceName);
			rs.close();
		} finally {
			if (stmt != null) stmt.close();
		}
		return sequence;
	}

	private final long last_value;
	private final long start_value;
	private final long increment_by;
	private final long max_value;
	private final long min_value;
	private final long cache_value;
	private final boolean is_cycled;

	private Sequence(String sequenceName, ResultSet rs, Schema schema, String owner) throws SQLException {
		//super(rs.getString("sequence_name"), schema, owner); // postgresql bug? not always consistent with pg_class.relname
		super(sequenceName, schema, owner);
		this.last_value = rs.getLong("last_value");
		this.start_value = rs.getLong("start_value");
		this.increment_by = rs.getLong("increment_by");
		this.max_value = rs.getLong("max_value");
		this.min_value = rs.getLong("min_value");
		this.is_cycled = rs.getBoolean("is_cycled");
		this.cache_value = rs.getLong("cache_value");
	}

	@Override
	protected StringBuilder appendCreateSql(StringBuilder buf, DataFilter dataFilter) {
		buf.append("CREATE SEQUENCE ");
		buf.append(getName());
		if (increment_by != 1) {
			buf.append(" INCREMENT BY ");
			buf.append(increment_by);
		}
		buf.append(" MINVALUE ");
		buf.append(min_value);
		buf.append(" MAXVALUE ");
		buf.append(max_value);
		if (is_cycled)
			buf.append(" CYCLE");
		if (cache_value > 1) {
			buf.append(" CACHE ");
			buf.append(cache_value);
		}
		buf.append(" START ");
		buf.append(start_value);
		buf.append(";\n");
		if (dataFilter.dumpData(schema.getName(), name)) {
			buf.append("SELECT setval('");
			buf.append(getName());
			buf.append("',");
			buf.append(last_value);
			buf.append(") ;\n");
		}
		return buf;
	}

	@Override
	protected StringBuilder appendCreateSql(StringBuilder buf) {
		throw new UnsupportedOperationException();
	}
}
