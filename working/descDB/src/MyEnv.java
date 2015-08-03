

import com.sleepycat.je.*;
import com.sleepycat.bind.*;

import java.io.File;
import java.util.*;
import java.util.Map.*;



public class MyEnv {
	
	private static EnvironmentConfig myEnvConfig;
	private DatabaseConfig myDbConfig;
	
	private Environment myEnv;
	
	public final static boolean READONLY = true;
	public final static boolean NOT_READONLY = false;
	
	// myDbHash is used to stored db instance in this environment 
	private HashMap<String, Database> myDbHashHandle = new HashMap<String,Database>();
	
	
	public MyEnv() {}
	
	public void setup (File envHome, boolean readOnly) 
		throws DatabaseException {
		
		//create environment configuration and environment instance
		myEnvConfig = new EnvironmentConfig();
		myEnvConfig.setReadOnly(readOnly);
		myEnvConfig.setAllowCreate(!readOnly);
		
		myEnv = new Environment(envHome, myEnvConfig);
	}
	
	
	
	
	public Database setDatabase(String dbName, boolean readOnly) 
		throws DatabaseException {
		
		myDbConfig = new DatabaseConfig();
		myDbConfig.setReadOnly(readOnly);
		myDbConfig.setAllowCreate(!readOnly);
		
		Database db = myEnv.openDatabase(null, dbName, myDbConfig);
		myDbHashHandle.put(dbName, db);
		return db;
	}
	
	
	
	public Environment getEnv () {
		return myEnv;
	}
	
	public Database getDb (String dbName, boolean readOnly) throws DatabaseException {

		myDbConfig = new DatabaseConfig();
		myDbConfig.setReadOnly(readOnly);
		myDbConfig.setAllowCreate(!readOnly);
			
		return myEnv.openDatabase(null, dbName, myDbConfig);
	}
	
	
	public List<String> getDatabaseNames() {
		return myEnv.getDatabaseNames();
	}
	
	
	
	
	

	public void closeDb(String dbName) {
		Database db = myDbHashHandle.get(dbName);
		if ( db!= null ) {
			db.close();
		}
	}
	

	public void close() {
		
		if (myEnv !=null) {
			try {
				Iterator<Entry<String,Database>> it = myDbHashHandle.entrySet().iterator();
				while (it.hasNext()) {
					Database thisDb = it.next().getValue();
					if (thisDb != null ) {
						thisDb.close();
					}
				}
				myEnv.close();
			} catch (DatabaseException dbe) {
				System.err.println("Error closing Envorionment" + dbe.toString() );
			}
			
		}
	}
	
	
	public void removeDb(String dbName) throws Exception {
		
		List<String> dbNameList = getDatabaseNames();
				
		if (dbNameList.contains(dbName)) {
			
			closeDb(dbName);
			myEnv.removeDatabase(null, dbName);
			myDbHashHandle.remove(dbName);
		} else {
			throw new Exception("No such table");
		}
		
	}
	
	
	
	
	public void removeDbAll() {
		Iterator<String> it = myEnv.getDatabaseNames().iterator();
		while(it.hasNext() ) {
			String dbName = it.next();
			closeDb(dbName);
			myEnv.removeDatabase(null, dbName.toString() );
		}
	}
	
}