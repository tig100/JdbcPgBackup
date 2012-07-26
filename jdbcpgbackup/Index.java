/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class Index extends DbBackupObject {

	static class IndexFactory implements DBOFactory<Index> {

		@Override
		public Iterable<Index> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
			List<Index> indexes = new ArrayList<Index>();
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						//				"SELECT * FROM pg_indexes WHERE schemaname = ?");
						// duplicate the pg_indexes view definition but skipping the primary indexes
						"SELECT c.relname AS tablename, i.relname AS indexname, " +
						"pg_get_indexdef(i.oid) AS indexdef " +
						"FROM pg_index x " +
						"JOIN pg_class c ON c.oid = x.indrelid " +
						"JOIN pg_class i ON i.oid = x.indexrelid " +
						//"LEFT JOIN pg_namespace n ON n.oid = c.relnamespace " +
						//"LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace " +
						"WHERE c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" " +
						"AND NOT x.indisprimary AND c.relnamespace = ?");
				stmt.setInt(1, schema.getOid());
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					indexes.add(new Index(rs.getString("indexname"), schema, rs.getString("tablename"), rs.getString("indexdef")));
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}

			/*// too slow, get primary keys from pg_constraint instead, use pg_get_constraintdef()
		ZipBackup.timerStart("primary keys");
		DatabaseMetaData metaData = con.getMetaData();
		rs = metaData.getPrimaryKeys(null, schema.getName(), null);
		Map<String,PrimaryKey> pkeys = new HashMap<String,PrimaryKey>();
		while (rs.next()) {
			String tableName = rs.getString("TABLE_NAME");
			String indexName = rs.getString("PK_NAME");
			PrimaryKey pkey = pkeys.get(indexName);
			if (pkey == null) {
				pkey = new PrimaryKey(indexName, schema, tableName);
				pkeys.put(indexName, pkey);
			}
			pkey.columns.put(Integer.valueOf(rs.getInt("KEY_SEQ")), rs.getString("COLUMN_NAME"));		
		}
		rs.close();
		indexes.addAll(pkeys.values());
		ZipBackup.timerEnd("primary keys");
			 */		
			return indexes;
		}

		@Override
		public Index getDbBackupObject(Connection con, String indexName, Schema schema) throws SQLException {
			Index index = null;
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement("SELECT * FROM pg_indexes WHERE schemaname = ? " +
						"AND indexname = ?");
				stmt.setString(1, schema.getName());
				stmt.setString(2, indexName);
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					index = new Index(rs.getString("indexname"), schema, rs.getString("tablename"), rs.getString("indexdef"));
				} else {
					throw new RuntimeException("no such index: " + indexName);
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return index;
		}
	}

	static class CachingIndexFactory extends CachingDBOFactory<Index> {

		private final Table.CachingTableFactory tableFactory;

		protected CachingIndexFactory(Schema.CachingSchemaFactory schemaFactory, Table.CachingTableFactory tableFactory) {
			super(schemaFactory);
			this.tableFactory = tableFactory;
		}

		@Override
		protected PreparedStatement getAllStatement(Connection con)	throws SQLException {
			return con.prepareStatement(
					"SELECT x.indrelid AS table_oid, i.relname AS indexname, " +
							"pg_get_indexdef(i.oid) AS indexdef, " +
							"i.relnamespace AS schema_oid " +
							"FROM pg_index x " +
							"JOIN pg_class i ON i.oid = x.indexrelid " +
							"WHERE i.relkind = 'i'::\"char\" " +
					"AND NOT x.indisprimary ");
		}

		@Override
		protected Index newDbBackupObject(Connection con, ResultSet rs, Schema schema) throws SQLException {
			Table table = tableFactory.getTable(rs.getInt("table_oid"));
			return new Index(rs.getString("indexname"), schema, table.getName(), rs.getString("indexdef"));
		}

	}

	protected final String tableName;
	private final String definition;

	private Index(String name, Schema schema, String tableName, String definition) {
		super(name, schema, null); // no owner (always same as table)
		this.tableName = tableName;
		this.definition = definition;
	}

	@Override
	String getSql() {
		return definition.replace(" ON " + schema.getName() + ".", " ON ") + " ;\n";  // remove schema name
	}

	@Override
	protected StringBuilder appendCreateSql(StringBuilder buf) {
		throw new UnsupportedOperationException();
	}

	/* not used because too slow getting them
	private final static class PrimaryKey extends Index {

		private SortedMap<Integer,String> columns = new TreeMap<Integer,String>();

		private PrimaryKey(String name, Schema schema, String tableName) {
			super(name, schema, tableName, null);
		}

		@Override
		public String getSql() {
			StringBuilder buf = new StringBuilder();
			buf.append("ALTER TABLE ");
			buf.append(tableName);
			buf.append(" ADD CONSTRAINT ");
			buf.append(name);
			buf.append(" PRIMARY KEY (");
			for (String column : columns.values()) {
				buf.append(column);
				buf.append(",");
			}
			buf.deleteCharAt(buf.length()-1);
			buf.append(");\n");
			return buf.toString();
		}

	}
	 */

}
