import java.io.*;
import java.util.*;
import java.util.Map.*;
import java.util.regex.*;
import java.text.*;

import com.sleepycat.je.*;

public class InsertManager implements MyConstants {
	
	private static ConstraintManager myCM = new ConstraintManager();
	private static SelectManager mySM = new SelectManager();
	private static TupleManager myTM = new TupleManager();
	private static MyEnv myEnv;
	
	public InsertManager() {}
	
	public InsertManager(MyEnv env) { myEnv = env;}
	
	/*
	 * 
	 * 		Insert method
	 * 
	 * 
	 */
	
	private ArrayList<String> recRefedPtList = null;
	private ArrayList<String> checkedRefPtList = null; 
	
	private HashMap<String, ArrayList<String> > refingTupleHash = null;  
	private HashMap<String, HashMap<String,String> > refingTableColDefHash = null;
	private HashMap<String,String> colNullityHash = null;
	
	
	public void doInsert(String tableName, ArrayList<String> insColList, ArrayList<String> insValList)
					throws Exception {
		Database tableDb = myEnv.setDb(tableName, READONLY);
	
		HashMap<String,String> allKVHash;

		try {
			allKVHash = myCM.getAllHashFromDb(tableDb);
		} finally {
        	tableDb.close();
        }
	
		HashMap<String,String> colDefHash = myCM.getHashByKind(allKVHash, COLDEF_PREFIX);
		ArrayList<String> allTpList = myCM.getTupleListFromHash(allKVHash, TUPLE_PREFIX, false);
		ArrayList<String> refingPtList = myCM.getRefPtListFromHash(allKVHash, REFINGPT_PREFIX);
		
		
		HashMap<String,String> insColValHash = new HashMap<String,String>();
		
		
		// Order of element in ArrayList is very important! 
		ArrayList<String> orgColList = new ArrayList<String>() ;
		ArrayList<String> priColList = new ArrayList<String>();
		ArrayList<String> priCVPairList = new ArrayList<String>();
		ArrayList<String> forColList = new ArrayList<String>();
		 
		//read original column definition hash
		for(int i=1; i<=colDefHash.size(); i++ ) {
			String[] nowColDefConstraint = myCM.getConstraintWithId(colDefHash, COLDEF_PREFIX, i);
			
			//colDefConstraint : "id name colDef"
			String nowColName = nowColDefConstraint[1];
			orgColList.add(nowColName);
			// initialize all value with null
			insColValHash.put(nowColName, "null");
			
			String nowColDef = nowColDefConstraint[2];
			String keyType = myCM.getColDefAttr(nowColDef, KEYTYPE);
			if(keyType.contains("PRI")){
				priColList.add(nowColName);
			}
			if(keyType.contains("FOR")){
				forColList.add(nowColName);
			}
		}
		
		// Setup insert column list with two cases.
		if(insColList == null) { // If insColList is not defined :
			if( orgColList.size() != insValList.size() ) {
				throw new InsertTypeMismatchError();
			}
			insColList = orgColList;
			
		} else { // If insColList is obtained by user defined :
			 
			if( insColList.size() != insValList.size() ) {
				throw new InsertTypeMismatchError();
			} else if( insColList.size() > orgColList.size() ) {
				throw new InsertTypeMismatchError();
			} else if(insColList.size() <= orgColList.size() ) {

				// Case modify
				ArrayList<String> modifiedICList = new ArrayList<String>();
				for(String nowICName : insColList) {
					String modifiedICName = SQLParser.autoCaseModify(orgColList, nowICName);
					if(modifiedICName == null) {
						throw new InsertColumnExistenceError(nowICName);
					} else {
						modifiedICList.add(modifiedICName);
					}
				}
				insColList = modifiedICList;
			}
		}
		
		
		
		for(int i=0; i<insColList.size(); i++) {
			insColValHash.put(insColList.get(i), insValList.get(i) );
		}
		
		
		
		
		for(Entry<String,String> colAndValueIt : insColValHash.entrySet()) {
			String nowColName = colAndValueIt.getKey();
			String nowValue = colAndValueIt.getValue();
			
			//check integrity constraint with referencing column
			String[] nowColDefConstraint = myCM.getConstraintWithName(colDefHash, COLDEF_PREFIX, nowColName);        
			String nowColDef = nowColDefConstraint[2];
			if(nowValue.equals("null")) {
				if( !isNullable(nowColDef) ) { throw new InsertColumnNonNullableError(nowColName); }
			} else {
				if( !isEqualDataType(nowColDef, nowValue) ) { throw new InsertTypeMismatchError(); }
				
				String dt = myCM.getColDefAttr(nowColDef,DATATYPE);
				if(dt.startsWith("char") ){
					//updated truncated string
					nowValue = charTruncating(dt,nowValue);
					insColValHash.put(nowColName, nowValue ); 
				}
			}
			
			String nowCVPair = myTM.mergeCVPair(nowColName, nowValue);
			
			if(priColList.contains(nowColName)) {
				priCVPairList.add(nowCVPair);
			}
			
			// If now column and value is foreign key, then
			// check integrity constraint with referenced table. 
			// Assumption : column names are unique in certain table 
			// If nowValue is null, then pass this check.
			if(forColList.contains(nowColName) && !nowValue.equals("null")) {
				ArrayList<String> nowColRefingPt = myCM.getRefPtByKind(refingPtList, SUBJECT, nowColName);    
				for(String nowRefingPt : nowColRefingPt) {
					String nowRefedTable = myCM.getRefPtPart(nowRefingPt, OBJECT_TABLENAME);
					String nowRefedCol = myCM.getRefPtPart(nowRefingPt, OBJECT);
					
					String nowCVPairRenamed = myTM.mergeCVPair(nowRefedCol, nowValue);
					
					// Open referenced database and read all hash
					tableDb = myEnv.setDb(nowRefedTable, READONLY);
					HashMap<String,String> nowRefedAllKVHash;
					try{
						nowRefedAllKVHash = myCM.getAllHashFromDb(tableDb);
					} finally {
						tableDb.close();
					}
					
					// Read all tuples of referenced table
					HashMap<String,String> nowRefedTpHash = myCM.getHashByKind(nowRefedAllKVHash, TUPLE_PREFIX);
					
					// Check referential integrity constraint 
					boolean find = false;
					for(Entry<String,String> nowRefedTpEntryIt : nowRefedTpHash.entrySet() ){
						// Search the referenced tuple by nowRefedCol and nowValue and return the existence.
						String nowRefedTp = nowRefedTpEntryIt.getKey();
						String[] nowRefedTpArr = nowRefedTp.split("\"");
						ArrayList<String> nowRefedTpArrayList = new ArrayList<String>(Arrays.asList(nowRefedTpArr));
						if(nowRefedTpArrayList.contains(nowCVPairRenamed) ) {
							find = true;
							break;
						}
					}
					if(!find) {
						throw new InsertReferentialIntegrityError();
					}
				}
			}
			
		}
		
		
		// find duplicated primary key
		for(String tpIt : allTpList) {
			String[] nowTpArr = tpIt.split("\"");
			ArrayList<String> nowTpArrayList
				= new ArrayList<String>(Arrays.asList(nowTpArr));
			
			boolean hasEqualCV = true;
			for(String priCVPairIt : priCVPairList) {
				if(!nowTpArrayList.contains(priCVPairIt)){
					hasEqualCV = false;
					break;
				}
			}
			if(hasEqualCV){
				throw new InsertDuplicatePrimaryKeyError();
			}
		}
	
		String sKey = myTM.mergeTuple(insColValHash);
		String sValue = " ";
		
		DatabaseEntry theKey = new DatabaseEntry(sKey.getBytes("UTF-8"));
		DatabaseEntry theValue = new DatabaseEntry(sValue.getBytes("UTF-8"));
		
		tableDb = myEnv.setDb(tableName, NOT_READONLY);
		
		try {
			tableDb.put(null, theKey, theValue);
		} finally {
        	tableDb.close();
        }
        
		System.out.println("write complte, key : " + sKey +", value : " + sValue);
    	
    	
		
	}
	
	
	
	public boolean validDate(String date_s) throws SQLException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setLenient(false);
		try {
			sdf.parse(date_s);
		} catch (java.text.ParseException e) {
			throw new InvalidDateRangeError();
		}
		return true;
	}
	
	public String charTruncating(String charDef, String charValue) {
		String slen = charDef.substring(5, charDef.length()-1);
		int len = Integer.parseInt(slen);
		
		if(charValue.length()-2 > len) { //-2 means quote character
			String sub = charValue.substring(1, len+1);
			return new String("\'" + sub + "\'");
		} else {
			return charValue;
		}
	}
	
	public boolean isEqualDataType(String definition, String value) throws SQLException {
		String dt = myCM.getColDefAttr(definition, DATATYPE);
		if(dt.startsWith("char")) {
			dt = "char";
		}
		
		Matcher m = null;
		
		switch( dt ) {
			case "date" :
				Pattern datePattern = Pattern.compile("(\\d{1,4})(-)(\\d{1,2})(-)(\\d{1,2})");
				m = datePattern.matcher(value);
				if(m.matches() && !validDate(value) ) {
					throw new InvalidDateRangeError();
				}
				break;
				
			case "int" :
				Pattern intPattern = Pattern.compile("(-)?(\\d)+");
				m = intPattern.matcher(value);
				break;
				
			case "char" : 
				Pattern charPattern = Pattern.compile("\'([^\"\'\t\n\r])*\'");
				m = charPattern.matcher(value);
				break;
		}
		
		return m.matches();
	}
	
	public boolean isNullable(String definition) throws Exception {
		String nullity = myCM.getColDefAttr(definition, NULLITY);
		
		if(nullity.equals("N") ) {
			return false;
		} else if(nullity.equals("Y") ) {
			return true;
		} else {
			throw new Exception("Wrong nullity stored");
		}
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public void ____up_insert____down_delete____() {
		
	}
	
	
	
	/*
	 * 
	 * 
	 * 		Delete method
	 * 
	 * 
	 * 
	 */
	
	
	

	ArrayList<String> deleteTupleList = null;
	ArrayList<String> undeleteTupleList = null;
	
	// { tableName : tupleModifyHash }  {beforeTuple : afterTuple}
	HashMap<String, HashMap<String, String > > prereadyNullTupleHash = null;
	
	// { tableName : tupleModifyHash }  {beforeTuple : afterTuple}
	HashMap<String, HashMap<String, String > > readyNullTupleHash = null; 
	
	
	
	
	
	
	
	
	
	
	
	
	
	public int[] doDelete(String tableName, ArrayList<String> _whereTupleList) throws Exception {
		deleteTupleList = new ArrayList<String>();
		undeleteTupleList = new ArrayList<String>();
		
		recRefedPtList = new ArrayList<String>();
		
		refingTupleHash = new HashMap<String, ArrayList<String> >();  
		refingTableColDefHash = new HashMap<String, HashMap<String,String> >();
		
		/* //remove later
		System.out.println();
		System.out.println("target tuples");
		for(String str : _whereTupleList) {
			System.out.println(str);	
		}
		System.out.println();
		*/

		
		// 1. Open db and get referenced pointing list of this table
		Database tableDb = myEnv.setDb(tableName, READONLY);
		HashMap<String,String> allKVHash;

		try { allKVHash = myCM.getAllHashFromDb(tableDb); } 
		finally { tableDb.close(); }
	
		ArrayList<String> refedPtList = myCM.getRefPtListFromHash(allKVHash, REFEDPT_PREFIX);
		
		// If referenced pointer is not exist, then no need to check cascading delete.
		if(refedPtList == null || refedPtList.size() == 0) {
			int delCount = deleteTupleList(tableName, _whereTupleList);
			int undelCount = 0;
			int[] delNum = {delCount, undelCount};
			return delNum;
		}
		
		
		
		// 2. Find all referencing table column of this table
		// input : refedPtList; 
		// 		   { String refedPt = new String(prefix + "\"" + refedCol + "\"" + refingTable + "\"" + refingCol) }
		// output : refedPtCascadedList
		//			{ refedTable.refedCol<-refingTable.refingCol }
		// 			refingTableList
		//			{String tableName}

		ArrayList<String> refedPtCascadedList = this.searchRefedPtCascaded(tableName, refedPtList);
		ArrayList<String> refingTableList = this.searchRefingTableList(refedPtCascadedList);
		
		
		// 3. Set all column definition and tuple of referencing table
		// input : refingTableList
		// output : refingTupleHash ~ HashMap<String, ArrayList<String> >  
		//			{ tableName : thisRefingTupleList }
		// 			refingTableColDefHash ~ HashMap<String, HashMap<String,String> >
		//			{ tableName : colDefHash}  colDefHash = { colDefKey : colDef }
		
		setHashesFromReferencingTable(refingTableList);
		
		
		// 4. Set nullity of each referencing table column
		// input : refingTableColDefHash, refedPtCascadedList 
		// output : colNullityHash ~ HashMap<String,String>
		//			{ refingTable.refingCol : "Y"|"N" }
		
		colNullityHash = setNullity(refedPtCascadedList ); 
		
		// 5. Check each tuples which are referenced or not
		// if referenced and all referencing column is null Y -> delete (count)
		// if referenced and some referencing column is null N -> pass (count)
		// 
		
		readyNullTupleHash = new HashMap<String, HashMap<String, String > >();
		
		for(String tupleIt : _whereTupleList) {
			checkedRefPtList = new ArrayList<String>() ;
			boolean canDelete = true;
			prereadyNullTupleHash = new HashMap<String, HashMap<String, String > >();  
			
			for(String refPtIt : refedPtCascadedList ) {
				if( checkedRefPtList.contains(refPtIt)) {
					continue;
				}	
				
				String nowRefedTableCol = getKindOfCascadedRefedPt(refPtIt, 0);
				
				String nowVal = myTM.getValWithColOfTuple(tupleIt, nowRefedTableCol);
				
				if( nowVal.equals("null") ) {
					checkedRefPtList.add(refPtIt);
					continue;
				}
				
				if ( nowVal != null ) {
					if( !cascadingDeleteCheck(refPtIt, nowVal) ){
						canDelete = false;
						undeleteTupleList.add(tupleIt);
						break;
					}
				}
		
			}
			
			if(canDelete) {
				deleteTupleList.add(tupleIt);
				updateReadyNullTupleHash(prereadyNullTupleHash);
			}
			
		}
		
		/* //remove later
		System.out.println();
		System.out.println("after referential check");
		for(String str : deleteTupleList) {
			System.out.println(str);	
		}
		System.out.println();
		*/
		
		deleteTupleList(tableName, deleteTupleList);
		updateTupleToDb();
		
		int delCount = deleteTupleList.size();
		int undelCount = undeleteTupleList.size();
		int[] delNum = {delCount, undelCount};
		return delNum;
		
	}
	
	
	
	
	// Recursive method   
	// ***   Important   *** 
	// *** Do not modify ***
	public boolean cascadingDeleteCheck(String refPt, String refedVal) throws SQLException {
		
		if(!checkedRefPtList.contains(refPt)) {
			checkedRefPtList.add(refPt);
		} else {
			return true;
		}	
		
		String refingTableCol = getKindOfCascadedRefedPt(refPt, 1);
		
		String refingTable = getKindOfAppended(refingTableCol,0);
		
		ArrayList<String> refingTupleList = refingTupleHash.get(refingTable);
		String nullity = colNullityHash.get(refingTableCol);
		
		HashMap<String,String> tupleModifyHash = prereadyNullTupleHash.get(refingTable);
		if(tupleModifyHash == null) {
			tupleModifyHash = new HashMap<String,String>();
		}
		
		ArrayList<String> thisCascadedRefedPtList = getRefedPtListByRefedTableCol(refingTableCol);
		
		if(thisCascadedRefedPtList == null) {

			if(nullity.equals("N")) {
				for(String refingTupleIt : refingTupleList){
					String refingVal = myTM.getValWithColOfTuple(refingTupleIt, refingTableCol);
					if(refingVal != null && refedVal.equals(refingVal)) {
						return false;
					}
				}
				return true;
				
			} else if(nullity.equals("Y")) {
				for(String refingTupleIt : refingTupleList){
					String refingVal = myTM.getValWithColOfTuple(refingTupleIt, refingTableCol);
					if(refingVal != null && refedVal.equals(refingVal)) {
						
						String updateTuple;
						if(tupleModifyHash.containsKey(refingTupleIt)){
							String previousUpdatedTuple = tupleModifyHash.get(refingTupleIt);
							updateTuple = myTM.updateTuple(previousUpdatedTuple, refingTableCol, "null");
						} else {
							updateTuple = myTM.updateTuple(refingTupleIt, refingTableCol, "null");
						}
						tupleModifyHash.put(refingTupleIt, updateTuple);
						prereadyNullTupleHash.put(refingTable, tupleModifyHash);
						
					}
				}
				return true;
				
			} else {
				throw new SQLException("Wrong nullity stored");
			}
			

			
			
			
		} else {         // if(thisCascadedRefedPtList != null) 
		
			if(nullity.equals("N")) {
				for(String refingTupleIt : refingTupleList){
					String refingVal = myTM.getValWithColOfTuple(refingTupleIt, refingTableCol);
					if(refingVal != null && refedVal.equals(refingVal)) {
						return false;
					} 
				}
				return true;
				
			} else if(nullity.equals("Y")) {
				boolean thisRefingOccur = false;
				
				for(String refingTupleIt : refingTupleList){
					String refingVal = myTM.getValWithColOfTuple(refingTupleIt, refingTableCol);
					if(refingVal != null && refedVal.equals(refingVal)) {
						thisRefingOccur = true;
						
						String updateTuple;
						if(tupleModifyHash.containsKey(refingTupleIt)){
							String previousUpdatedTuple = tupleModifyHash.get(refingTupleIt);
							updateTuple = myTM.updateTuple(previousUpdatedTuple, refingTableCol, "null");
						} else {
							updateTuple = myTM.updateTuple(refingTupleIt, refingTableCol, "null");
						}
						tupleModifyHash.put(refingTupleIt, updateTuple);
						prereadyNullTupleHash.put(refingTable, tupleModifyHash);
						
					}
				}
				
				if(!thisRefingOccur) {
					return true;
				}
				
				boolean cascadedResult = true;
				for(String cascadedRefPtIt : thisCascadedRefedPtList) {
					if(!cascadingDeleteCheck(cascadedRefPtIt, refedVal) ) {
						cascadedResult = false;
						break;
					}
				}
				
				if( !cascadedResult ){
					return false;
				}
				
				return true;
				
			} else {
				throw new SQLException("Wrong nullity stored");
			}
			
			
		} 
		
		
		
	}

	
	
	
	
	
	
	public HashMap<String,String>  setNullity (ArrayList<String> _refedPtCascadedList ) 
						throws SQLException, Exception {
		HashMap<String,String>  result = new HashMap<String,String> ();
		
		for(String refPtIt : _refedPtCascadedList) {
			String nowRefingTableCol = getKindOfCascadedRefedPt(refPtIt, 1);
			String nowRefingTable = getKindOfAppended(nowRefingTableCol,0);
			String nowRefingCol = getKindOfAppended(nowRefingTableCol,1);
			
			HashMap<String,String> nowColDefHash = refingTableColDefHash.get(nowRefingTable);
			String nowColDef = myCM.getConstraintWithName(nowColDefHash, COLDEF_PREFIX, nowRefingCol)[2];
			String nullity = myCM.getColDefAttr(nowColDef, NULLITY);

			result.put(nowRefingTableCol, nullity);
		}
		
		return result;
		
	}
	
	
	
	public void setHashesFromReferencingTable (ArrayList<String> tableNameList) 
				throws SQLException, Exception {
		
		for(String tableNameIt : tableNameList) {
			Database tableDb = myEnv.setDb(tableNameIt, READONLY);
			HashMap<String,String> allKVHash;
	
			try { allKVHash = myCM.getAllHashFromDb(tableDb); } 
			finally { tableDb.close(); }
		
			HashMap<String,String> colDefHash = myCM.getHashByKind(allKVHash, COLDEF_PREFIX);
			ArrayList<String> allTpList = myCM.getTupleListFromHash(allKVHash, TUPLE_PREFIX, false);
			allTpList = mySM.appendTableNameToAllTuple(tableNameIt, allTpList);
			
			refingTableColDefHash.put(tableNameIt, colDefHash);
			refingTupleHash.put(tableNameIt, allTpList);   
		}
		
	}

	
	

	
	public ArrayList<String> searchRefedPtCascaded(String tableName, ArrayList<String> _refedPtList ) 
						throws SQLException, Exception {
		ArrayList<String> result = new ArrayList<String>();
		
		//initialize of recRefedPtList
		for(String initialRefedPtIt : _refedPtList) {
			String nowRefedCol = myCM.getRefPtPart(initialRefedPtIt, SUBJECT);
			String nowRefingTable = myCM.getRefPtPart(initialRefedPtIt, OBJECT_TABLENAME); 
			String nowRefingCol = myCM.getRefPtPart(initialRefedPtIt, OBJECT);
			
			String nowRefedTableCol = appendTableName(tableName, nowRefedCol);
			String nowRefingTableCol = appendTableName(nowRefingTable, nowRefingCol);
			String nowMergedCascadedRefPt = mergeCascadedRefPt(nowRefedTableCol, nowRefingTableCol );
			
			result.add(nowMergedCascadedRefPt);
			recRefedPtList.add(nowMergedCascadedRefPt);
		}
		
		// calling recursive method
		// to set up recRefedPtList (global variable)
		for(String refPt : result) {
			recSearchRefedPt(refPt);
		}
		
		return recRefedPtList;
	}
	
	

	// Recursive method   
	// ***   Important   *** 
	// *** Do not modify ***
	public void recSearchRefedPt(String refPt) 	throws SQLException, Exception {
			
		String nowRefingTableCol = getKindOfCascadedRefedPt(refPt, 1);
		
		String nowRefingTable = getKindOfAppended(nowRefingTableCol,0);
		String nowRefingCol = getKindOfAppended(nowRefingTableCol,1);
		
		ArrayList<String> thisColRefedPtList = getRefedPtListOfOneCol(nowRefingTable, nowRefingCol );
		if(thisColRefedPtList == null)
			return;
		
		for(String cascadedRefPtIt : thisColRefedPtList ) {
			if(!recRefedPtList.contains(cascadedRefPtIt)) {
				recRefedPtList.add(cascadedRefPtIt);
				recSearchRefedPt( cascadedRefPtIt );
			}
		}
		
	}
	

	
	public ArrayList<String> searchRefingTableList(ArrayList<String> _refedPtCascadedList ) {
		ArrayList<String> result = new ArrayList<String>();
		
		for(String refedPtIt : _refedPtCascadedList) {
			String nowRefingTableCol = getKindOfCascadedRefedPt(refedPtIt, 1);
			String nowRefingTable = getKindOfAppended(nowRefingTableCol,0);
			if( !result.contains(nowRefingTable)) {
				result.add(nowRefingTable);
			}
		}
		
		return result;
	}
	

	public ArrayList<String> getRefedPtListByRefedTableCol(String tableCol) {
		ArrayList<String> result = new ArrayList<String>();
		
		for(String storedRefedPtIt : recRefedPtList ) {
			String nowRefedPt = this.getKindOfCascadedRefedPt(storedRefedPtIt, 0);
			if(tableCol.equals(nowRefedPt) ) {
				result.add(storedRefedPtIt);
			}
		}
		
		if(result != null && result.size()!= 0)
			return result;
		else 
			return null;
	}
	
	

	
	
	public ArrayList<String> getRefedPtListOfOneCol(String tableName, String colName) throws SQLException, Exception {
		ArrayList<String> result = new ArrayList<String>();
		ArrayList<String> refedPtList = getRefedPtListOfTable(tableName);
		String refedTableAndCol = this.appendTableName(tableName, colName);
		
		if(refedPtList == null || refedPtList.size()== 0)
			return null;
		
		for(String refedPtIt : refedPtList ){
			String nowCol = myCM.getRefPtPart(refedPtIt, SUBJECT);
			if(colName.equalsIgnoreCase(nowCol)) {
				String nowRefingTable = myCM.getRefPtPart(refedPtIt, OBJECT_TABLENAME); 
				String nowRefingCol = myCM.getRefPtPart(refedPtIt, OBJECT);
				String refingTableAndCol = appendTableName(nowRefingTable,nowRefingCol);
				String nowRefedPt = this.mergeCascadedRefPt(refedTableAndCol, refingTableAndCol);
				result.add(nowRefedPt);
			}
		}
		
		if(result != null && result.size()!= 0)
			return result;
		else 
			return null;
	}
	
	public ArrayList<String> getRefedPtListOfTable(String tableName) throws SQLException, Exception {
		Database tableDb = myEnv.setDb(tableName, READONLY);
		HashMap<String,String> allKVHash;

		try { allKVHash = myCM.getAllHashFromDb(tableDb); } 
		finally { tableDb.close(); }
	
		ArrayList<String> refedPtList = myCM.getRefPtListFromHash(allKVHash, REFEDPT_PREFIX);
		
		if(refedPtList != null && refedPtList.size()!= 0)
			return refedPtList;
		else 
			return null;	
		
	}
	
	
	
	
	

	public void updateReadyNullTupleHash(HashMap<String, HashMap<String,String> > prereadyNullTupleHash) {
		Iterator<Entry<String, HashMap<String,String> > > it = prereadyNullTupleHash.entrySet().iterator();
		while(it.hasNext()){
			Entry<String, HashMap<String,String> > nowEntry = it.next();
			String nowTable = nowEntry.getKey();
			HashMap<String,String> nowModifyTupleHash = nowEntry.getValue();
			
			HashMap<String,String> orgModifyTupleHash;
			orgModifyTupleHash = readyNullTupleHash.get(nowTable);
			if(orgModifyTupleHash == null) {
				orgModifyTupleHash = new HashMap<String,String>();
			}
			
			
			Iterator<Entry<String,String> > it2 = nowModifyTupleHash.entrySet().iterator();
			while(it2.hasNext()) {
				Entry<String, String > nowEntry2 = it2.next();
				String nowUpdateTargetTuple = nowEntry2.getKey();
				String nowUpdateModifyTuple = nowEntry2.getValue();
				orgModifyTupleHash.put(nowUpdateTargetTuple, nowUpdateModifyTuple);
				
			}
			
			readyNullTupleHash.put(nowTable, orgModifyTupleHash);
			
		}
		
	}
	
	

	public void updateTupleToDb() throws SQLException, Exception {
		Iterator<Entry<String, HashMap<String,String> > > it = readyNullTupleHash.entrySet().iterator();
		while(it.hasNext()){
			Entry<String, HashMap<String,String> > nowEntry = it.next();
			String nowTable = nowEntry.getKey();
			HashMap<String,String> nowModifyTupleHash = nowEntry.getValue();
			
			Database db = myEnv.setDb(nowTable, NOT_READONLY);
			Cursor cursor = db.openCursor(null, null);
			
			OperationStatus retVal;
			
			try{
				for(Entry<String,String> nowChangingTuple : nowModifyTupleHash.entrySet() ) {
					
					String beforeTuple = nowChangingTuple.getKey();
					String modiTuple = nowChangingTuple.getValue();
					
					beforeTuple = myTM.detachTableNameOfTuple(beforeTuple);
					modiTuple = myTM.detachTableNameOfTuple(modiTuple);
					
					//remove later
					System.out.println();
					System.out.println("before and after");
					System.out.println(beforeTuple);
					System.out.println(modiTuple);
					System.out.println();
					
					
					DatabaseEntry theKey = new DatabaseEntry(beforeTuple.getBytes("UTF-8") ); 
					DatabaseEntry theData = new DatabaseEntry();
					
					retVal = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
					if(retVal == OperationStatus.SUCCESS) {
						cursor.delete();
						theKey = new DatabaseEntry(modiTuple.getBytes("UTF-8") );
						cursor.put(theKey, theData);
					} else {
						throw new SQLException("Error occured in modify tuple. Cannot find original tuple");
					}
				}
			} finally {
				cursor.close();
				db.close();
			}
			
		}
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public int deleteTupleList(String tableName, ArrayList<String> _whereTupleList) throws Exception {
		Database tableDb = myEnv.setDb(tableName, NOT_READONLY);
		Cursor cursor = tableDb.openCursor(null, null);
		OperationStatus retVal;
		int count = 0;
		
		try{
			for(String nowTuple : _whereTupleList) {
				nowTuple = myTM.detachTableNameOfTuple(nowTuple);
				
				DatabaseEntry theKey = new DatabaseEntry(nowTuple.getBytes("UTF-8") ); 
				DatabaseEntry theData = new DatabaseEntry();
				
				retVal = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
				if(retVal == OperationStatus.SUCCESS) {
					count++;
					cursor.delete();
				}
			}
		} finally {
			cursor.close();
			tableDb.close();
		}
		
		return count;
		
	}
	
	
	
	
	
	

	public String mergeCascadedRefPt(String refedTableCol, String refingTableCol) {
		return new String(refedTableCol + "<-" + refingTableCol);
	}
	
	public String getKindOfCascadedRefedPt(String cascadedRefPt, int kind) {
		//0:refedPt, 1:refingPt
		String[] splited = cascadedRefPt.split("<-");
		if(kind == 0) { return splited[0]; }
		else { return splited[1]; }
	}

	public String appendTableName(String tableName, String colName) {
		return new String(tableName+'.'+colName);
	}
	
	public String getKindOfAppended(String tableAndCol, int kind) {
		//0:tableName, 1:colName
		String[] splited = tableAndCol.split("\\.");
		if(kind == 0) { return splited[0]; } 
		else { return splited[1]; }
	}
	
	
	
	
}
