import com.sleepycat.je.*;
import com.sleepycat.bind.*;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.Map.*;

public class MyEnv {

	private static EnvironmentConfig myEnvConfig;
	private DatabaseConfig myDbConfig;

	private Environment myEnv;

	public final static boolean READONLY = true;
	public final static boolean NOT_READONLY = false;

	// myDbHash is used to stored database handle in this environment
	private HashMap<String, Database> myDbHashHandle = new HashMap<String, Database>();

	public MyEnv() {
	}

	public void setup(File envHome, boolean readOnly) throws DatabaseException {

		// create environment configuration and environment instance
		myEnvConfig = new EnvironmentConfig();
		myEnvConfig.setReadOnly(readOnly);
		myEnvConfig.setAllowCreate(!readOnly);

		myEnv = new Environment(envHome, myEnvConfig);
	}

	public Database setDb(String dbName, boolean readOnly)
			throws DatabaseException {

		dbName = autoCaseModify(dbName);

		myDbConfig = new DatabaseConfig();
		myDbConfig.setSortedDuplicates(true);
		myDbConfig.setReadOnly(readOnly);
		myDbConfig.setAllowCreate(!readOnly);

		Database db = myEnv.openDatabase(null, dbName, myDbConfig);
		if (myDbConfig.getAllowCreate()) {
			myDbHashHandle.put(dbName, db);
		}
		return db;
	}


	public String autoCaseModify(String dbName) throws DatabaseException {

		List<String> dbNameList = this.getDbNames();

		for (int i = 0; i < dbNameList.size(); i++) {
			if (dbName.equalsIgnoreCase(dbNameList.get(i))) {
				return dbNameList.get(i);
			}
		}

		return dbName;
	}

	public Environment getEnv() {
		return myEnv;
	}

	public List<String> getDbNames() {
		return myEnv.getDatabaseNames();

	}

	public boolean hasKindOf(Database db, String prefix) throws SQLException,
	Exception {

		Cursor cursor = null;
		OperationStatus retVal;
		DatabaseEntry theKey;
		DatabaseEntry theData;
		try {

			theKey = new DatabaseEntry(prefix.getBytes("UTF-8"));
			theData = new DatabaseEntry();

			cursor = db.openCursor(null, null);

			retVal = cursor
					.getSearchKeyRange(theKey, theData, LockMode.DEFAULT);

		} finally {
			cursor.close();
		}

		if (retVal == OperationStatus.SUCCESS) {
			String keyStr = new String(theKey.getData(), "UTF-8");
			if (keyStr.startsWith(prefix)) {
				return true;
			}
		}

		return false;

	}

	public void closeDb(String dbName) {

		dbName = autoCaseModify(dbName);

		Database db = myDbHashHandle.get(dbName);
		if (db != null) {
			db.close();
		}
	}

	public void close() {

		if (myEnv != null) {
			try {
				Iterator<Entry<String, Database>> it = myDbHashHandle.entrySet().iterator();
				while (it.hasNext()) {
					Database thisDb = it.next().getValue();
					if (thisDb != null) {
						thisDb.close();
					}
				}
				myEnv.close();
			} catch (DatabaseException dbe) {
				System.err.println("Error closing Envorionment"
						+ dbe.toString());
			}

		}
	}

	public void removeDb(String dbName) throws SQLException, Exception {

		List<String> dbNameList = getDbNames();

		if (dbNameList.contains(dbName)) {

			closeDb(dbName);
			myEnv.removeDatabase(null, dbName);
			myDbHashHandle.remove(dbName);

		} else {
			throw new NoSuchTable();
		}

	}

	public void removeDbAll() {
		Iterator<String> it = this.getDbNames().iterator();
		while (it.hasNext()) {
			String dbName = it.next();
			closeDb(dbName);
			myEnv.removeDatabase(null, dbName);
		}
	}
	
	public int deleteAllTp(String tableName) throws Exception {
		Database db = this.setDb(tableName, NOT_READONLY);
		Cursor cursor = db.openCursor(null, null);
		int count = 0;
		
		try {
			String TUPLE_PREFIX = "-tp";
			DatabaseEntry theKey = new DatabaseEntry(TUPLE_PREFIX.getBytes("UTF-8") ); 
			DatabaseEntry theData = new DatabaseEntry();
			
			OperationStatus retVal = cursor.getSearchKeyRange(theKey, theData, LockMode.DEFAULT);
			while(retVal == OperationStatus.SUCCESS){
				String searchedKey = new String(theKey.getData(),"UTF-8");
				if(searchedKey.startsWith(TUPLE_PREFIX)) {
					count++;
					cursor.delete();
					retVal = cursor.getNext(theKey, theData, LockMode.DEFAULT);
				} else {
					break;
				}
			}
			
		} finally {
			cursor.close();
			db.close();
		}
		
		return count; 
	}

	
	
	
	
	
	
	
	
	
	
	
}














