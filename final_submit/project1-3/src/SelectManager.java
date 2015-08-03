import java.io.*;
import java.util.*;
import java.util.Map.*;
import java.util.regex.*;
import java.text.*;
import java.text.ParseException;

import com.sleepycat.je.*;


public class SelectManager implements MyConstants {

	private static ConstraintManager myCM = new ConstraintManager(); 
	private static TupleManager myTM = new TupleManager(); 
	private static MyEnv myEnv;
	private static ArrayList<String> staticCrossedTupleList = new ArrayList<String>();
	private static ArrayList<String> fromTableList = new ArrayList<String>();
	private static ArrayList<String> allColOfFromTable = new ArrayList<String>();
	private static ArrayList<String> staticAllTupleList = new ArrayList<String>();
	private static HashMap<String, HashMap<String,String> > fromTableAllColDefHash 
										= new HashMap<String, HashMap<String,String> >();
	private static ArrayList<String> whereTupleList;
	private static String whereQuery = null;
	
	private static ThreeValueLogic myTVL = new ThreeValueLogic();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public SelectManager() {}

	public SelectManager(MyEnv env) { myEnv = env; }


	public void setFromTableList(ArrayList<String> _fromTableList) {
		fromTableList = _fromTableList;
	}
	
	public void setAllColOfFromTable(ArrayList<String> _allColOfFromTable) {
		allColOfFromTable = _allColOfFromTable;
	}
	
	public void setAllTupleList(ArrayList<String> _allTupleList) {
		staticAllTupleList = _allTupleList;
	}
	
	public void setFromTableAllColDefHash(HashMap<String, HashMap<String,String> > _fromTableAllColDefHash ) {
		fromTableAllColDefHash = _fromTableAllColDefHash;
	}
	
	
	
	
	
	
	
	
	//Make cartesian product tuple list 
	public ArrayList<String> crossingTable( HashMap<String, ArrayList<String> > _allTableTupleHash ) {
		staticCrossedTupleList = new ArrayList<String>();
		crossingTupleRC("", _allTableTupleHash);
		removeFirstQuote();
		return staticCrossedTupleList;
	}

	// Recursive method   
	// ***   Important   *** 
	// *** Do not modify ***
	private String crossingTupleRC(String beforeTp,  HashMap<String, ArrayList<String> > _allTableTupleHash ) {
		if(_allTableTupleHash.size()==0) { 
			return beforeTp;
		} else {
			Entry<String, ArrayList<String> > nowEntry = _allTableTupleHash.entrySet().iterator().next();
			String nowName = nowEntry.getKey();
			ArrayList<String> nowTupleList = nowEntry.getValue();
			_allTableTupleHash.remove(nowName);

			String crossedTp;
			String rcCrossedTp;
			for(String nowTuple : nowTupleList) {
				crossedTp = mergeTuple(beforeTp,nowTuple);
				rcCrossedTp = crossingTupleRC(crossedTp, _allTableTupleHash);
				if(rcCrossedTp != null) {
					staticCrossedTupleList.add(rcCrossedTp );
				}
			}
			_allTableTupleHash.put(nowName, nowTupleList);
			return null;
		}

	}

	public void removeFirstQuote() {
		if(staticCrossedTupleList != null) {
			ArrayList<String> modifiedList = new ArrayList<String>();
			for(String str : staticCrossedTupleList) {
				String modifiedStr = str.substring(1);
				modifiedList.add(modifiedStr);
			}
			
			staticCrossedTupleList = new ArrayList<String>(modifiedList);
		}
		
	}
	
	public String mergeTuple(String beforeTp, String nextTp) {
		return new String(beforeTp+"\""+ nextTp);
	}


	public ArrayList<String> getAllTableNameAppendedCol(ArrayList<String> tableNameList) throws SQLException, Exception {

		ArrayList<String> result = new ArrayList<String>();

		for(String nowTableName : tableNameList) {

			// Read all hash from table database, 
			// get column definition hash, 
			// and get column name list in turns. 
			Database nowDb = myEnv.setDb(nowTableName, READONLY);
			HashMap<String,String> nowAllHash;
			try {
				nowAllHash = myCM.getAllHashFromDb(nowDb);
			} finally {
				nowDb.close();
			}

			HashMap<String,String> nowColDefHash = myCM.getHashByKind(nowAllHash, COLDEF_PREFIX);
			ArrayList<String> nowColList = myCM.getColNameList(nowColDefHash);

			for(String nowCol : nowColList) {
				result.add( myTM.appendTableName( nowTableName, nowCol ) );
			}
		}
		return result;
	}

	public ArrayList<String> appendTableNameToAllSelCol(ArrayList<String> selColList, ArrayList<String> fromTableList) 
			throws SQLException, Exception {

		ArrayList<String> allColList = getAllTableNameAppendedCol(fromTableList);
		ArrayList<String> result = new ArrayList<String>();

		for(String nowSelCol : selColList ) {
			int matching = 0;
			String[] splitedSelCol = myTM.splitNameOfAppended(nowSelCol);
			// split nowSelCol into tableName, colName

			if(splitedSelCol[0] != null) {
				// 1. If tableName declared, 
				// compare tableName with fromTableList 
				// , case modifying
				String mTableName = SQLParser.autoCaseModify(fromTableList, splitedSelCol[0]);
				if( mTableName == null ) { throw new SelectTableExistenceError(splitedSelCol[0]); }

				// , and check colName of column existence  (ambiguous error do not occur)
				for(String nowAllCol : allColList) {
					if(nowSelCol.equalsIgnoreCase(nowAllCol) ) {
						matching++; 
						result.add(nowAllCol);
					}
				}

				// non existence column & ambiguous column error
				if(matching != 1 ) { throw new SelectColumnResolveError(nowSelCol); }

			} else {
				// 2. Else if tableName not declared,
				// search colName in allColList
				// , case modifying
				// , and check the occurrence

				for(String nowAllCol : allColList) {
					String[] splitedAllCol = myTM.splitNameOfAppended(nowAllCol);
					if(splitedSelCol[1].equalsIgnoreCase(splitedAllCol[1]) ) {
						matching++;
						result.add(nowAllCol);
					}
				}

				// column existence error and ambiguous error 
				if( matching != 1 ) { throw new SelectColumnResolveError(splitedSelCol[1]); }
			}
		}

		return result;
	}

	public ArrayList<String> appendTableNameToAllTuple(String tableName, ArrayList<String> tupleList) 
			throws SQLException, Exception {
		ArrayList<String> result = new ArrayList<String>();

		for(String nowTuple : tupleList) {
			String appendedTp = myTM.appendTableNameOfTuple(tableName, nowTuple);
			result.add(appendedTp);
		}

		return result;

	}

	public String searchAndAppendCol(String col) throws SQLException {
		
		int count = 0;
		String tableName_colName = null;
		String serached_TableName_ColName = null;
		for(String tableName : fromTableList) {
			tableName_colName = myTM.appendTableName(tableName, col);
			for(String nowOrg : allColOfFromTable) {
				if(nowOrg.startsWith(tableName)) {
					if(tableName_colName.equalsIgnoreCase(nowOrg)) {
						serached_TableName_ColName = nowOrg;
						count++;
					} 
				}
			}
		}
		
		if(count > 1) {
			throw new WhereAmbiguousReference(); 
		} else if (count == 0) {
			throw new WhereColumnNotExist();
		}
		
		return serached_TableName_ColName;
	}

	public ThreeValue compareOP(String op1, String operator, String op2, String tuple) throws SQLException, Exception {
		
		int op1Type = typeSwith(op1);
		int op2Type = typeSwith(op2);
		
		if(op1Type != op2Type) { throw new WhereIncomparableError(); }

		String op1Str = getOperandValue(op1, tuple);
		String op2Str = getOperandValue(op2, tuple);
		
		if(op1Str.equals("null") || op2Str.equals("null")) {
			return ThreeValue.UNKNOWN;
		}
		
		switch( op1Type ) {
			case INT_VAL :
				int op1Int = Integer.parseInt(op1Str);
				int op2Int = Integer.parseInt(op2Str);
				switch(operator) {
					case ">" : 
						if(op1Int > op2Int) return ThreeValue.TRUE;
						break;
					case "<" : 
						if(op1Int < op2Int) return ThreeValue.TRUE;
						break;
					case "=" : 
						if(op1Int == op2Int) return ThreeValue.TRUE;
						break;
					case "!=" : 
						if(op1Int != op2Int) return ThreeValue.TRUE;
						break;
					case ">=" : 
						if(op1Int >= op2Int) return ThreeValue.TRUE;
						break;
					case "<=" : 
						if(op1Int <= op2Int) return ThreeValue.TRUE;
						break;
					default : 
				}
				
				break;
			case DATE_VAL :
				sdf.setLenient(true);
				Date op1Date = sdf.parse(op1Str);
				Date op2Date = sdf.parse(op2Str);
				switch(operator) {
					case ">" : 
						if( op1Date.compareTo(op2Date) > 0 ) return ThreeValue.TRUE;
						break;
					case "<" : 
						if( op1Date.compareTo(op2Date) < 0 ) return ThreeValue.TRUE;
						break;
					case "=" : 
						if( op1Date.compareTo(op2Date) == 0 ) return ThreeValue.TRUE;
						break;
					case "!=" : 
						if( op1Date.compareTo(op2Date) != 0 ) return ThreeValue.TRUE;
						break;
					case ">=" : 
						if( op1Date.compareTo(op2Date) >= 0 ) return ThreeValue.TRUE;
						break;
					case "<=" : 
						if( op1Date.compareTo(op2Date) <= 0 ) return ThreeValue.TRUE;
						break;
					default : 
				}
				break;
			
			case CHAR_STR :
				
				switch(operator) {
					case ">" : 
						if(op1Str.compareTo(op2Str) > 0 ) return ThreeValue.TRUE;
						break;
					case "<" : 
						if(op1Str.compareTo(op2Str) < 0 ) return ThreeValue.TRUE;
						break;
					case "=" : 
						if(op1Str.compareTo(op2Str) == 0 ) return ThreeValue.TRUE;
						break;
					case "!=" : 
						if(op1Str.compareTo(op2Str) != 0 ) return ThreeValue.TRUE;
						break;
					case ">=" : 
						if(op1Str.compareTo(op2Str) >= 0 ) return ThreeValue.TRUE;
						break;
					case "<=" : 
						if(op1Str.compareTo(op2Str) <= 0 ) return ThreeValue.TRUE;
						break;
					default : 
				}
		}
		

		return ThreeValue.FALSE;
	}

	public String getOperandValue(String op, String tuple) throws SQLException {
		
		if(op.startsWith(PREFIX_CR) ) {
			op = remove_CRVAL_Prefix(op);
			// Tuple has already table name appended column name (table1.col1:val1"table1.col2:val2"...)
			// see FromClause in SQLParser.jj
			// Column reference has already table name appended column name 
			// see CompOperand in SQLParser.jj
			return myTM.getValWithColOfTuple(tuple, op);
 		
		} else {
			return remove_CRVAL_Prefix(op);
		}
		
	}
	
	public int typeSwith(String compVal) throws SQLException {

		Pattern datePattern = Pattern.compile("(\\d{1,4})(-)(\\d{1,2})(-)(\\d{1,2})");
		Pattern intPattern = Pattern.compile("(-)?(\\d)+");
		Pattern charPattern = Pattern.compile("\'([^\"\'\t\n\r])*\'");

		if(compVal.startsWith(PREFIX_CR) ) {
			compVal = remove_CRVAL_Prefix(compVal);
			String[] tableAndCol = myTM.splitNameOfAppended( compVal );
			HashMap<String, String> colDefHash = fromTableAllColDefHash.get(tableAndCol[0]);
			String colDef = myCM.getColDef(colDefHash, tableAndCol[1]);
			String type = myCM.getColDefAttr(colDef, DATATYPE);
			
			if(type.startsWith("int")) {
				return INT_VAL;
			} else if (type.startsWith("char")) {
				return CHAR_STR;
			} else if (type.startsWith("date")) {
				return DATE_VAL;
			} else {
				throw new SQLException("Stored Type error");
			}
 		
		} else {
			compVal = remove_CRVAL_Prefix(compVal);
			
			if(datePattern.matcher(compVal).matches()) {
				// Date validity is already checked in insert operation
				return DATE_VAL;

			} else if (intPattern.matcher(compVal).matches()) {
				return INT_VAL;

			} else if (charPattern.matcher(compVal).matches() ) {
				return CHAR_STR;

			} else {
				throw new SQLException("Stored Type error");
			}

		}

	}

	public String remove_CRVAL_Prefix(String word) {
		//only use in column reference or comparison value string
		String result = null;
		if(word.startsWith(PREFIX_CR) ) {
			result = word.substring(PREFIX_CR.length(), word.length());
		} else {
			result = word.substring(PREFIX_VAL.length(), word.length());
		}
		return result;
	}
	






	
	
	
	
	
	
	
	
	
	
	
	
	
	/*
	 * 	Where clause calling method
	 * 
	 *  1. setWhereQuery : initialize query string 
	 *  2. pickTupleByWhereQuery : call where clause by input all tuples
	 * 
	 */
		

	public void setWhereQuery(String query) {
		whereQuery= query;
		return;
	}

	public ArrayList<String> pickTupleByWhereQuery(String query, ArrayList<String> _allTupleList)
						throws SQLException, Exception {
		
		this.setAllTupleList(_allTupleList);
		whereTupleList = new ArrayList<String>();
		
		for(String nowTuple : staticAllTupleList) {
			setWhereQuery(query);
			WhereClause(nowTuple);
		}
		
		return whereTupleList;
	}



	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/*
	 * 
	 * 	Query processing method
	 * 
	 *  1. consume
	 *  2. lookahead
	 * 	3. getFirstToken
	 * 
	 */
	
	
	
	public boolean consume(String token) {
		if(whereQuery.startsWith(token) ) {
			whereQuery = whereQuery.substring(token.length(), whereQuery.length());
			return true;
		}
		return false;
	}
	
	public boolean lookAhead(String token) {
		if( whereQuery.startsWith(token) ) {
			return true;
		}
		return false;
	}
	
	public String getFirstToken() {
		String[] splited = whereQuery.split("\"");
		consume(splited[0]+"\"");
		return splited[0];
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/*
	 * 	Where Clause 
	 * 	: pseudo code of javacc code.
	 *  : query string is repeatedly setted by pickTupleByWhereQuery
	 *  : query is global vairable string
	 *  
	 */
	
	
	
	
	
	public void WhereClause(String tuple) throws SQLException, Exception {
		ThreeValue bve = ThreeValue.FALSE;
			
		bve = BooleanValueExpression(tuple);
		if(bve == ThreeValue.TRUE) {
			whereTupleList.add(tuple);
		}
	}
	
	
	ThreeValue BooleanValueExpression(String tuple) throws SQLException, Exception {
		ThreeValue bt;
		ThreeValue result;
		
		result = BooleanTerm(tuple);
		while( consume(TOK_OR) ) {
			bt = BooleanTerm(tuple);
			result = myTVL.or(result,bt);
		}
		return result;
		
	}
		
		
	
	ThreeValue BooleanTerm(String tuple) throws SQLException, Exception   {
		ThreeValue result;
		ThreeValue bf;
		
	    result = BooleanFactor(tuple);
	    while( consume(TOK_AND) ) {
			bf = BooleanFactor(tuple);
			result = myTVL.and(result,bf);
	    }
		return result;
	}
	
	
	
	ThreeValue BooleanFactor(String tuple) throws SQLException, Exception  {
		ThreeValue result;
		boolean not = false;

		if (consume(TOK_NOT) ) {
	   		not = true;
		}
	    result = BooleanTest(tuple);
	    
    	if(not) {
    		return myTVL.neg(result);
    	} else {
    		return result;
    	}
    	
	}

	
	ThreeValue BooleanTest(String tuple) throws SQLException, Exception  {
		
		if( lookAhead(TOK_LEFTPAR) ) {
			return ParenthesizedBooleanExpression(tuple);
		} else {
			return Predicate(tuple);
		}
	}
	
	ThreeValue ParenthesizedBooleanExpression(String tuple) throws SQLException, Exception  {
		ThreeValue bt;
		ThreeValue result;
		
		consume(TOK_LEFTPAR);
		result = BooleanTerm(tuple);
		while(consume(TOK_OR)) {
			bt = BooleanTerm(tuple);
			result = myTVL.or(result,bt);
		}
		consume(TOK_RIGHTPAR);
		
		return result;
	}
	
	
	ThreeValue Predicate(String tuple) throws SQLException, Exception  {
		
		if ( lookAhead( PREFIX_VAL ) || lookAhead( PREFIX_CR )   ) { 
			return ComparisonPredicate(tuple);
		} else {
			return NullPredicate(tuple);
		}
	}

		
		
	
	ThreeValue ComparisonPredicate(String tuple) throws SQLException, Exception {
		String opnd1;
		String opnd2;
		String operator;
		
	    opnd1 = CompOperand();
	    operator = getFirstToken();
	    opnd2 = CompOperand();
    	return compareOP(opnd1, operator, opnd2, tuple);
	}
	
	String CompOperand()  throws SQLException {
		
		if( lookAhead(PREFIX_VAL)) {
			consume(PREFIX_VAL);
	    	String ft = getFirstToken();
	    	return new String(PREFIX_VAL + ft);
		} else if (lookAhead(PREFIX_CR) ){
			consume(PREFIX_CR);
			String ft = getFirstToken();
	    	return new String(PREFIX_CR + ft);
		} else {
			throw new SQLException("Wrong compare operand");
		}
	}
	
	
	
	
	
	
	
	ThreeValue NullPredicate(String tuple) throws SQLException {
		String tableNameAppendedColName;
		String nullOp;
		
		tableNameAppendedColName = getFirstToken();
	    nullOp = getFirstToken();
	    
	    return nullPredicating(tableNameAppendedColName, nullOp, tuple);
	    
	}

	
	

	public ThreeValue nullPredicating(String colName, String nullOp, String tuple) {
		
		String val = myTM.getValWithColOfTuple(tuple, colName);
		
		if(val.equals("null") && nullOp.equals("is null")) {
			return ThreeValue.TRUE;
		} else if(!val.equals("null") && nullOp.equals("is not null")) {
			return ThreeValue.TRUE; 
		}

		return ThreeValue.FALSE;
	}

	

	
	
	


}