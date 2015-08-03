import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.*;
import java.text.*;

public class TupleManager implements MyConstants {
	

	public TupleManager() {}

	// Get value of specified column in a tuple
	public String getValWithColOfTuple(String tuple, String colName) {
		String[] cvPairs = splitTuple(tuple);  //c1:v1"c2:v2"...
		
		for(String cvPairIt : cvPairs) { 
			String[] nowCVPair = splitCVPair(cvPairIt);
			if(colName.equals(nowCVPair[0])) { 
				//caution : now contain, but equals!
				//be aware of appended or not appended case 
				return nowCVPair[1];
			}
		}
		
		return null;
	}
	
	
	// Append table names ahead of all column names of a tuple 
	public String appendTableNameOfTuple(String tableName, String tuple) {
		String[] cvPairs = splitTuple(tuple);
		String result = new String(TUPLE_PREFIX + "\"");
		
		String[] nowCVPair;
		for(String cvPairIt : cvPairs ) {
			nowCVPair = splitCVPair(cvPairIt); // 0:column name, 1:value
			String appendedColName =  appendTableName(tableName, nowCVPair[0]);
			String appendedCVPair = mergeCVPair(appendedColName, nowCVPair[1]);
			result = result.concat(appendedCVPair + "\"");
		}
		
		return result.substring(0, result.length()-1);
	}
	
	// Append table name to column name
	public String appendTableName(String tableName, String colName) {
		return new String(tableName+'.'+colName);
	}
	
	
	
	
	// Detach table name of each cvPairs in tuple
	// cvPair : column value pair
	public String detachTableNameOfTuple(String tuple) throws Exception {
		String[] cvPairs = splitTuple(tuple);
		String result = new String(TUPLE_PREFIX + "\"");
		
		String[] nowCVPair;
		for(String cvPairIt : cvPairs ) {
			nowCVPair = splitCVPair(cvPairIt); // 0:column name, 1:value
			String detachedColName = splitNameOfAppended(nowCVPair[0])[1];
			String detachedCVPair = mergeCVPair(detachedColName, nowCVPair[1]);
			result = result.concat(detachedCVPair + "\"");
		}
		
		return result.substring(0, result.length()-1);
	}
	
	
	
	
	
	// Update value of tuple with respect to column
	public String updateTuple(String tuple, String colName, String updateVal) throws SQLException {
		String[] splited = splitTuple(tuple);
		String[] updateSplited = new String[splited.length];
		
		for( int i=0; i<splited.length; i++){
			String cvPairIt = splited[i]; // tableName.col:value
			String[] cvPair = splitCVPair(cvPairIt); // 0: tableName.col  1:value  
			if(cvPair[0].equals(colName)) {
				cvPairIt = this.mergeCVPair(cvPair[0], updateVal );
			}
			updateSplited[i] = cvPairIt;
		}
		return mergeTuple(updateSplited);
	}
	
	
	
	
	
	
	
	
	public String mergeTuple(String[] splitedTuple) {
		String result = new String(TUPLE_PREFIX+"\"");
		for(String sp : splitedTuple) {
			result = result.concat(sp + "\"");
		}

		return result.substring(0, result.length()-1);
	}
	

	public String mergeTuple (HashMap<String,String> _colValHash) throws SQLException {
		
		String result = new String(TUPLE_PREFIX+"\"");
		
		for(Entry<String,String> colDefKeyEntryIt : _colValHash.entrySet() ) {
			String nowCol = colDefKeyEntryIt.getKey();
			String nowVal = colDefKeyEntryIt.getValue();
			String merged = mergeCVPair(nowCol,nowVal);
			result = result.concat(merged +"\"" );
		}
		
		return result.substring(0, result.length()-1);
		
	}
	
	public String mergeCVPair(String col, String val) {
		return new String(col+':'+val);
	}
	
	
	
	
	
	
	
	

	public String[] splitNameOfAppended(String doubtColName) throws SQLException {
		//0:tableName, 1:colName
		String[] result = new String[2];
		if(doubtColName.contains(".")){
			String[] splited = doubtColName.split("\\.");
			if(splited.length == 2) {
				result[0] = splited[0];
				result[1] = splited[1];
			} else {
				throw new SQLException("Wrong appended tableName and colName");
			}
		} else {
			result[0] = null;
			result[1] = doubtColName;
		}
		
		return result;
	}
	
	
	public String[] splitCVPair(String cvPair) {
		return cvPair.split(":");
	}
	

	public String[] splitTuple(String tuple) {
		String[] splited = tuple.split("\"");
		String[] result;
		if(splited[0].equals(TUPLE_PREFIX) ){
			result = Arrays.copyOfRange(splited, 1, splited.length);
		} else {
			result = Arrays.copyOf(splited, splited.length);
		}
		return result;
	}
	
	
}