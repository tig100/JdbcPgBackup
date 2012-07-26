/*	Copyright (c) 2012	Tomislav Gountchev <tomi@gountchev.net>	*/

package jdbcpgbackup;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class JdbcPgBackup {

	public static final String USAGE =
			"Usage: JdbcPgBackup -m dump|restore [-h hostname] [-p port] [-t (timing)] " +
					"[-d database] [-U user] [-P password] [-f filename] [-o (schema only)] " +
					"[-s schema[,schema...]] [-n schema[,schema...]] [-b batchsize]";

	private static Map<String,String> parseArgs(String[] args) {
		Map<String,String> params = new HashMap<String,String>();
		for (int i = 0; i<args.length; i++) {
			if (args[i].startsWith("-") && args[i].length() == 2) {
				char option = args[i].charAt(1);
				switch(option) {
				case 'm':
					params.put("mode", args[++i]);
					break;
				case 'h':
					params.put("hostname", args[++i]);
					break;
				case 'p':
					params.put("port", args[++i]);
					break;
				case 'd':
					params.put("database", args[++i]);
					break;
				case 'U':
					params.put("user", args[++i]);
					break;
				case 'P':
					params.put("password", args[++i]);
					break;
				case 'f':
					params.put("filename", args[++i]);
					break;
				case 's':
					params.put("schemas", args[++i]);
					break;
				case 'n':
					params.put("toschemas", args[++i]);
					break;
				case 'b':
					params.put("batch", args[++i]);
					break;
				case 't':
					params.put("debug", "true");
					break;
				case 'o':
					params.put("nodata", "true");
					break;
				default:
					throw new RuntimeException("invalid parameter: " + args[i]);
				}
			} else throw new RuntimeException("invalid parameters");
		}
		return params;
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println(USAGE);
			System.exit(1);
		}
		Map<String, String> params = null;
		try {
			params = parseArgs(args);
		} catch (RuntimeException e) {
			System.err.println(USAGE);
			System.err.println(e.getMessage());
			System.exit(1);
		}
		ZipBackup backup = new ZipBackup(params);
		try {
			String[] schemas = null;
			String[] toSchemas = null;
			String schemasParam = params.get("schemas");
			if (schemasParam != null) {
				schemas = schemasParam.split(",");
			}
			String toSchemasParam = params.get("toschemas");
			if (toSchemasParam != null) {
				toSchemas = toSchemasParam.split(",");
				if (schemas == null || schemas.length == 0 || schemas.length != toSchemas.length)
					throw new RuntimeException("non-matching source schema (-s) and destination schema (-n) parameters");
			}
			String mode = params.get("mode");

			if ("true".equals(params.get("debug")))
				ZipBackup.setTimingOutput(System.err);

			if ("dump".equals(mode)) {
				boolean nodata = "true".equals(params.get("nodata"));
				DataFilter dataFilter = nodata ? DataFilter.NO_DATA : DataFilter.ALL_DATA;
				String batchS = params.get("batch");
				int batch = batchS == null ? ZipBackup.DEFAULT_BATCH_SIZE : Integer.parseInt(batchS);
				if (schemas == null) {
					backup.dumpAll(dataFilter, batch);
				} else {
					backup.dump(Arrays.asList(schemas), dataFilter);
				}
			} else if ("restore".equals(mode)) {
				if (schemas == null) {
					backup.restoreAll();
				} else if (toSchemas == null) {
					for (String schema : schemas) {
						backup.restoreSchema(schema);
					}
				} else {
					for (int i=0; i<schemas.length; i++) {
						backup.restoreSchemaTo(schemas[i], toSchemas[i]);
					}
				}
			} else throw new RuntimeException("invalid mode: " + mode);
		} catch (RuntimeException e) {
			System.err.println("backup failed: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(1);
		}
		System.exit(0);
	}

}
