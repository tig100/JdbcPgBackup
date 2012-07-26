/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class Constraint extends DbBackupObject {

	static class ConstraintFactory implements DBOFactory<Constraint> {

		@Override
		public Iterable<Constraint> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
			List<Constraint> constraints = new ArrayList<Constraint>();
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT c.oid, c.conname, p.relname, pg_get_userbyid(p.relowner) AS owner, " +
								"pg_get_constraintdef(c.oid) AS constraintdef, c.contype " +
								"FROM pg_constraint c, pg_class p " +
								"WHERE c.connamespace = ? " +
								"AND c.conrelid = p.oid " +
						"ORDER BY c.contype DESC");
				stmt.setInt(1, schema.getOid());
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					constraints.add(new Constraint(rs.getString("conname"), schema, rs.getString("relname"),
							rs.getString("owner"), rs.getString("constraintdef"), rs.getString("contype").charAt(0)));
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return constraints;
		}

		@Override
		public Constraint getDbBackupObject(Connection con, String constraintName, Schema schema) throws SQLException {
			Constraint constraint = null;
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT c.oid, c.conname, p.relname, pg_get_userbyid(p.relowner) AS owner, " +
								"pg_get_constraintdef(c.oid) AS constraintdef, c.contype " +
								"FROM pg_constraint c, pg_class p " +
								"WHERE c.connamespace = ? " +
								"AND c.conrelid = p.oid " +
						"AND c.conname = ? ");
				stmt.setInt(1, schema.getOid());
				stmt.setString(2, constraintName);
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					constraint = new Constraint(constraintName, schema, rs.getString("relname"),
							rs.getString("owner"), rs.getString("constraintdef"), rs.getString("contype").charAt(0));
				} else {
					throw new RuntimeException("no such constraint: " + constraintName);
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return constraint;
		}

	}

	static class CachingConstraintFactory extends CachingDBOFactory<Constraint> {

		private final Table.CachingTableFactory tableFactory;

		protected CachingConstraintFactory(Schema.CachingSchemaFactory schemaFactory, Table.CachingTableFactory tableFactory) {
			super(schemaFactory);
			this.tableFactory = tableFactory;
		}

		@Override
		protected PreparedStatement getAllStatement(Connection con) throws SQLException {
			return con.prepareStatement(
					"SELECT c.oid, c.conname, c.conrelid AS table_oid, " +
							"pg_get_constraintdef(c.oid) AS constraintdef, " +
							"c.connamespace AS schema_oid, c.contype " +
					"FROM pg_constraint c");
		}

		@Override
		protected Constraint newDbBackupObject(Connection con, ResultSet rs, Schema schema) throws SQLException {
			Table table = tableFactory.getTable(rs.getInt("table_oid"));
			return new Constraint(rs.getString("conname"), schema, table.getName(),
					table.getOwner(), rs.getString("constraintdef"), rs.getString("contype").charAt(0));
		}

		// get the primary key constraints before the rest
		@Override
		public final Iterable<Constraint> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
			Iterable<Constraint> constraints = super.getDbBackupObjects(con, schema);
			List<Constraint> pklist = new ArrayList<Constraint>();
			List<Constraint> fklist = new ArrayList<Constraint>();
			for (Constraint constraint : constraints) {
				if (constraint.type == 'p') pklist.add(constraint);
				else fklist.add(constraint);
			}
			pklist.addAll(fklist);
			return pklist;
		}

	}

	private final String tableName;
	private final String definition;
	private final char type;

	private Constraint(String name, Schema schema, String tableName, String tableOwner, String definition, char type) {
		super(name, schema, tableOwner);
		this.tableName = tableName;
		this.definition = definition.replace(" REFERENCES " + schema.getName() + ".", " REFERENCES "); // remove schema name
		this.type = type;
	}

	@Override
	protected StringBuilder appendCreateSql(StringBuilder buf) {
		buf.append("ALTER TABLE ");
		buf.append(tableName);
		buf.append(" ADD CONSTRAINT ");
		buf.append(name);
		buf.append(" ");
		buf.append(definition);
		buf.append(" ;\n");
		return buf;
	}

}
