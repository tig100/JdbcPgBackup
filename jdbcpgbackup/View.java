/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class View extends DbBackupObject {

	static class ViewFactory implements DBOFactory<View> {

		@Override
		public Iterable<View> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
			List<View> views = new ArrayList<View>();
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT * FROM pg_views WHERE schemaname = ?");
				stmt.setString(1, schema.getName());
				ResultSet rs = stmt.executeQuery();
				while (rs.next()) {
					views.add(new View(rs.getString("viewname"), schema, rs.getString("viewowner"), rs.getString("definition")));
				}
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return views;
		}

		@Override
		public View getDbBackupObject(Connection con, String viewName, Schema schema) throws SQLException {
			View view = null;
			PreparedStatement stmt = null;
			try {
				stmt = con.prepareStatement(
						"SELECT * FROM pg_views WHERE schemaname = ? AND viewname = ?");
				stmt.setString(1, schema.getName());
				stmt.setString(2, viewName);
				ResultSet rs = stmt.executeQuery();
				if (rs.next())
					view = new View(viewName, schema, rs.getString("viewowner"), rs.getString("definition"));
				else
					throw new RuntimeException("no such view: " + viewName);
				rs.close();
			} finally {
				if (stmt != null) stmt.close();
			}
			return view;
		}
	}

	static class CachingViewFactory extends CachingDBOFactory<View> {

		protected CachingViewFactory(Schema.CachingSchemaFactory schemaFactory) {
			super(schemaFactory);
		}

		@Override
		protected final PreparedStatement getAllStatement(Connection con) throws SQLException {
			return con.prepareStatement(
					"SELECT c.relnamespace AS schema_oid, c.relname AS viewname, pg_get_userbyid(c.relowner) AS viewowner, " +
							"pg_get_viewdef(c.oid) AS definition " +
							"FROM pg_class c " +
					"WHERE c.relkind = 'v'::\"char\"");
			/*
					"SELECT * FROM pg_views " +
							"WHERE schemaname NOT LIKE 'pg_%' " +
							"AND schemaname <> 'information_schema'");
			 */
		}

		@Override
		protected final View newDbBackupObject(Connection con, ResultSet rs, Schema schema) throws SQLException {
			return new View(rs.getString("viewname"), schema,
					rs.getString("viewowner"), rs.getString("definition"));	
		}

	}


	private final String definition;

	private View(String name, Schema schema, String owner, String definition) {
		super(name, schema, owner);
		this.definition = definition;
	}

	@Override
	protected StringBuilder appendCreateSql(StringBuilder buf) {
		buf.append("CREATE VIEW ");
		buf.append(getName());
		buf.append(" AS ");
		buf.append(definition);
		buf.append(" ;\n");
		return buf;
	}

}
