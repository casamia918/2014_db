import com.sleepycat.je.*;

import java.io.*;
import java.util.*;
import java.util.Map.*;

public class descDB {

	public static final boolean NOT_READONLY = false;
	public static final boolean READONLY = true;

	private static File myEnvPath = new File(
			"/Users/HiChoi/dev/2014db/project1-2new/db");

	private static DatabaseEntry theKey;
	private static DatabaseEntry theData;
	private static Database thisDb;
	private static Cursor cursor;
	private static String sData;
	private static String sKey;
	private static OperationStatus retVal;

	private static MyEnv myEnv = new MyEnv();

	public static void main(String[] args) {
		try {
			myEnv.setup(myEnvPath, READONLY);
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

			while (true) {
				try {
					System.out.println("What table to show? : ");

					String tableName = br.readLine();

					if (tableName.equals("#quit")) {
						break;
					} else if (tableName.equals("#all")) {
						List<String> nameList = myEnv.getDatabaseNames();
						for (String nowTableName : nameList) {
							desc(nowTableName);
						}

					} else {
						desc(tableName);
					}

					System.out.println();

				} catch (Exception e) {
					System.out.println(e.toString());
				}

			}

		} finally {
			myEnv.close();
		}
	}

	public static void desc(String tableName)
			throws UnsupportedEncodingException {
		thisDb = myEnv.setDatabase(tableName, READONLY);
		cursor = thisDb.openCursor(null, null);

		theKey = new DatabaseEntry();
		theData = new DatabaseEntry();

		retVal = cursor.getFirst(theKey, theData, LockMode.DEFAULT);

		System.out.println();
		System.out.printf("[%s]%n", tableName);

		while (retVal == OperationStatus.SUCCESS) {
			sKey = new String(theKey.getData(), "UTF-8");
			sData = new String(theData.getData(), "UTF-8");

			System.out.printf("%-30s      %-50s %n", sKey, sData);

			retVal = cursor.getNext(theKey, theData, LockMode.DEFAULT);
		}

		cursor.close();
		thisDb.close();

	}

}