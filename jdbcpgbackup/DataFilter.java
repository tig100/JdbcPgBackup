package jdbcpgbackup;


public interface DataFilter {
	public boolean dumpData(String schema,String tableName);

	public static final DataFilter ALL_DATA = new DataFilter() {
		public boolean dumpData(String schema,String tableName) {
			return true;
		}
	};

	public static final DataFilter NO_DATA = new DataFilter() {
		public boolean dumpData(String schema,String tableName) {
			return false;
		}
	};
}
