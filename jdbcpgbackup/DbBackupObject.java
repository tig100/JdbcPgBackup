/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

abstract class DbBackupObject {

	protected final String name;
	protected final Schema schema;
	protected final String owner;

	protected DbBackupObject(String name, Schema schema, String owner) {
		this.name = name;
		this.schema = schema;
		this.owner = owner;
	}

	final String getName() {
		return name;
	}

	String getFullname() {
		return schema.getName() + "." + name;
	}

	final String getOwner() {
		return owner;
	}

	String getSql() {
		StringBuilder buf = new StringBuilder();
		if (!owner.equals(schema.getOwner())) {
			buf.append("SET ROLE ").append(owner);
			buf.append(" ;\n");
		}
		appendCreateSql(buf);
		if (!owner.equals(schema.getOwner())) {
			buf.append("SET ROLE ").append(schema.getOwner());
			buf.append(" ;\n");
		}
		return buf.toString();
	}

	protected abstract StringBuilder appendCreateSql(StringBuilder buf);

}
