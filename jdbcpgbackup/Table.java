/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

final class Table extends DbBackupObject {

	static class TableFactory implements DBOFactory<Table> {

		@Override
		public Iterable<Table> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
			ZipBackup.timerStart("tables");
			List<Table> tables = new ArrayList<Table>();
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement("SELECT pg_get_userbyid(c.relowner) AS tableowner, " +
						"c.relname AS tablename, c.oid AS table_oid " +
						"FROM pg_class c " +
						"WHERE c.relkind = 'r'::\"char\" AND c.relnamespace = ?");
				stmt.setInt(1, schema.getOid());
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					Table table = new Table(rs.getString("tablename"), schema, rs.getString("tableowner"));
					loadColumns(con, table, rs.getInt("table_oid"));
					tables.add(table);
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			ZipBackup.timerEnd("tables");
			return tables;
		}

		// does not load columns
		@Override
		public Table getDbBackupObject(Connection con, String tableName, Schema schema) throws SQLException {
			return getDbBackupObject(con, tableName, schema, false);
		}

		public Table getDbBackupObject(Connection con, String tableName, Schema schema, boolean loadColumns) throws SQLException {
			Table table = null;
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement("SELECT pg_get_userbyid(c.relowner) AS tableowner, c.oid AS table_oid " +
						"FROM pg_class c " +
						"WHERE c.relkind = 'r'::\"char\" AND c.relnamespace = ? " +
						"AND c.relname = ?");
				stmt.setInt(1, schema.getOid());
				stmt.setString(2, tableName);
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					table = new Table(tableName, schema, rs.getString("tableowner"));
					if (loadColumns) loadColumns(con, table, rs.getInt("table_oid"));
				} else {
					throw new RuntimeException("no such table: " + tableName);
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return table;
		}

		private void loadColumns(Connection con, Table table, int tableOid) throws SQLException {
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT a.attname,a.atttypid," +
								"a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod," +
								"row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, " +
								"pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,t.typtype " +
								"FROM pg_catalog.pg_attribute a " +
								"JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) " +
								"LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) " +
								"WHERE a.attnum > 0 AND NOT a.attisdropped " +
						"AND a.attrelid = ? ");
				stmt.setInt(1, tableOid);
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					table.columns.add(table.new Column((BaseConnection)con, rs));
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
		}

	}

	static class CachingTableFactory extends CachingDBOFactory<Table> {

		private final Map<Integer,Table> oidMap;

		protected CachingTableFactory(Schema.CachingSchemaFactory schemaFactory) {
			super(schemaFactory);
			oidMap = new HashMap<Integer,Table>();
		}

		@Override
		protected PreparedStatement getAllStatement(Connection con) throws SQLException {
			return con.prepareStatement("SELECT c.relnamespace AS schema_oid, c.relname AS tablename, " + 
					"pg_get_userbyid(c.relowner) AS tableowner, c.oid " +
					"FROM pg_class c " +
					"WHERE c.relkind = 'r'::\"char\"");
		}

		@Override
		protected Table newDbBackupObject(Connection con, ResultSet rs, Schema schema) throws SQLException {
			Table table = new Table(rs.getString("tablename"), schema, rs.getString("tableowner"));
			oidMap.put(rs.getInt("oid"), table);
			return table;
		}

		@Override
		protected void loadMap(Connection con) throws SQLException {
			ZipBackup.timerStart("tables");
			super.loadMap(con);
			ZipBackup.timerEnd("tables");
			loadColumns(con);
		}

		Table getTable(int table_oid) {
			return oidMap.get(table_oid);
		}

		private void loadColumns(Connection con) throws SQLException {
			ZipBackup.timerStart("load columns");
			ZipBackup.debug("begin loading columns...");
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT a.attrelid AS table_oid, a.attname, a.atttypid," +
								"a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull, a.atttypmod, " +
								"row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, " +
								"pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc, t.typtype " +
								"FROM pg_catalog.pg_attribute a " +
								"JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) " +
								"LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) " +
						"WHERE a.attnum > 0 AND NOT a.attisdropped ");
				int count = 0;
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					int oid = rs.getInt("table_oid");
					Table table = oidMap.get(oid);
					if (table != null) {
						table.columns.add(table.new Column((BaseConnection)con, rs));
						if (++count%100000 == 1) ZipBackup.debug("loaded " + count + " columns");
					}
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			ZipBackup.debug("end loading columns");
			ZipBackup.timerEnd("load columns");		
		}

	}


	/*
	private static void loadSequences(Connection con, Schema schema, Map<String,Table> tables) throws SQLException {
		ZipBackup.timerStart("load sequences");
		PreparedStatement stmt = con.prepareStatement(
				"SELECT c.relname AS sequencename, d.refobjsubid AS columnid, p.relname AS tablename " + 
				"FROM pg_class c, pg_depend d, pg_class p " +
				"WHERE d.refobjid = p.oid " +
				"AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = ?) " +
				"AND c.oid = d.objid AND c.relkind = 'S'");
		stmt.setString(1, schema.getName());
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			tables.get(rs.getString("tablename")).columns.get(rs.getInt("columnid")).setSequenceName(rs.getString("sequencename"));
		}
		rs.close();
		stmt.close();
		ZipBackup.timerEnd("load sequences");
	}
	 */

	private final Set<Column> columns = new TreeSet<Column>(
			new Comparator<Column>() { 
				public int compare(Column a, Column b) {
					return a.position - b.position;
				};
			});

	private Table(String name, Schema schema, String owner) {
		super(name, schema, owner);
	}

	@Override
	protected StringBuilder appendCreateSql(StringBuilder buf) {
		buf.append("CREATE TABLE ").append(getName());
		buf.append(" (");
		for (Column column : columns) {
			column.appendSql(buf);
			buf.append(",");
		}
		buf.deleteCharAt(buf.length()-1);
		buf.append(")");
		buf.append(" ;\n");
		for (Column column : columns) {
			column.appendSequenceSql(buf);
		}
		return buf;
	}

	void dump(Connection con, OutputStream os) throws SQLException, IOException {
		CopyManager copyManager = ((PGConnection)con).getCopyAPI();
		copyManager.copyOut("COPY " + getFullname() + " TO STDOUT BINARY", os);
	}

	void restore(InputStream is, Connection con) throws SQLException, IOException {
		CopyManager copyManager = ((PGConnection)con).getCopyAPI();
		copyManager.copyIn("COPY " + getFullname() + " FROM STDIN BINARY", is);
	}

	private static final Set<String> appendSizeTo = new HashSet<String>(
			Arrays.asList("bit", "varbit", "bit varying", "bpchar", "char", "varchar", "character", "character varying"));

	private static final Set<String> appendPrecisionTo = new HashSet<String>(
			Arrays.asList("time", "timestamp", "timetz", "timestamptz"));

	/*
	private static final Map<String,String> aliasMap = new HashMap<String,String>();
	static {
		aliasMap.put("serial","integer");
		aliasMap.put("serial4","integer");
		aliasMap.put("serial8","bigint");
		aliasMap.put("bigserial","bigint");
	}

	private static String getAlias(String type) {
		String alias = aliasMap.get(type);
		return alias != null ? alias : type;
	}
	 */

	private class Column {

		private final String name;
		private final String typeName;
		private /*final*/ int columnSize;
		private final int decimalDigits;
		private final int nullable;
		private final String defaultValue;
		private final boolean isAutoincrement;
		private final String sequenceName;
		private final int position;

		private Column(BaseConnection con, ResultSet rs) throws SQLException {

			int typeOid = (int)rs.getLong("atttypid");
			int typeMod = rs.getInt("atttypmod");

			position = rs.getInt("attnum");
			name = rs.getString("attname");
			typeName = con.getTypeInfo().getPGType(typeOid);
			decimalDigits = con.getTypeInfo().getScale(typeOid, typeMod);
			columnSize = con.getTypeInfo().getPrecision(typeOid, typeMod);
			if (columnSize == 0) {
				columnSize = con.getTypeInfo().getDisplaySize(typeOid, typeMod);
			}
			if (columnSize == Integer.MAX_VALUE) {
				columnSize = 0;
			}
			nullable = rs.getBoolean("attnotnull") ? java.sql.DatabaseMetaData.columnNoNulls : java.sql.DatabaseMetaData.columnNullable;
			String columnDef = rs.getString("adsrc");
			if (columnDef != null) {
				defaultValue = columnDef.replace("nextval('" + schema.getName() + ".", "nextval('"); // remove schema name
				isAutoincrement = columnDef.indexOf("nextval(") != -1;
			} else {
				defaultValue = null;
				isAutoincrement = false;
			}
			if (isAutoincrement) {
				PreparedStatement stmt = null;
				try {
					stmt = con.prepareStatement(
							"SELECT pg_get_serial_sequence( ? , ? ) AS sequencename");
					stmt.setString(1, getFullname());
					stmt.setString(2, name);
					ResultSet rs2 = stmt.executeQuery();
					if (rs2.next() && rs2.getString("sequencename") != null) {
						sequenceName = rs2.getString("sequencename").replace(schema.getName() + ".", "");
					} else {
						sequenceName = null;
					}
					rs2.close();
				} finally {
					if (stmt != null) stmt.close();
				}
			} else sequenceName = null;
		}

		private StringBuilder appendSql(StringBuilder buf) {
			buf.append(name).append(" ");
			buf.append(typeName);
			if (appendSizeTo.contains(typeName) && columnSize > 0) {
				buf.append( "(").append(columnSize).append(")");
			} else if (appendPrecisionTo.contains(typeName) || typeName.startsWith("interval")) {
				buf.append("(").append(decimalDigits).append(")");
			} else if ("numeric".equals(typeName) || "decimal".equals(typeName)) {
				buf.append("(").append(columnSize).append(",").append(decimalDigits).append(")");
			}
			if (defaultValue != null) {
				buf.append(" DEFAULT ").append(defaultValue);
			}
			if (nullable == java.sql.DatabaseMetaData.columnNoNulls) {
				buf.append(" NOT NULL");
			}
			return buf;
		}

		private StringBuilder appendSequenceSql(StringBuilder buf) {
			if (sequenceName == null) return buf;
			buf.append("ALTER SEQUENCE ");
			buf.append(sequenceName);
			buf.append(" OWNED BY ");
			buf.append(getName());
			buf.append(".").append(name);
			buf.append(" ;\n");
			return buf;
		}

	}

}
