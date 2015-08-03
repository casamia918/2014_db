import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.bind.serial.StoredClassCatalog;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.List;

public class MyEnv {
	
	private static EnvironmentConfig myEnvConfig;
	private DatabaseConfig myDbConfig;
	
	private Environment myEnv;
	// myDbHash is used to stored db instance in this environment 
	
	private HashMap<String, Database> myDbHashHandle = new HashMap<String,Database>();
	//remove hash and replace to environment.getDatabaseNames()
	
	
	private StoredClassCatalog myClassCatalog;
	
	public MyEnv() {}
	
	public void setup (File envHome, boolean readOnly) 
		throws DatabaseException {
		
		//create environment config and environment instance
		myEnvConfig = new EnvironmentConfig();
		myEnvConfig.setReadOnly(readOnly);
		myEnvConfig.setAllowCreate(!readOnly);
		
		myEnv = new Environment(envHome, myEnvConfig);
		
		//create class catalog database and instance
		Database myClassCatalogDb = setDatabase("MyClassCatalogDb", readOnly);
		myClassCatalog = new StoredClassCatalog(myClassCatalogDb);
		
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
	
	public Database getDb (String dbName, boolean readOnly) 
		throws DatabaseException {
			
		myDbConfig = new DatabaseConfig();
		myDbConfig.setReadOnly(readOnly);
		myDbConfig.setAllowCreate(!readOnly);
			
		return myEnv.openDatabase(null, dbName, myDbConfig);
	}
	
	public StoredClassCatalog getClassCatalog() {
		return myClassCatalog;
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
	
}
