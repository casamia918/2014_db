import java.util.Arrays;
import java.util.HashMap;
import java.util.List;



enum KeyType {
	PRI, FOR, BOTH ;
}





public class TableInfo {
	
	private String tableName;
	private HashMap<String, ColInfo> colInfoHash = new HashMap<String, ColInfo>();
	private String[] priColList;
	private String[] forColList;
	private List<ForKey> forKeyList;
	
	public TableInfo () {}
	
	public TableInfo (String tableName) {
		this.tableName = tableName;
	}
	
	public void setTableName (String tableName) {
		this.tableName = tableName;
	}
	
	public void setCol(String colName, String dataType, boolean isNullable) {
		ColInfo newCol = new ColInfo(colName);
		newCol.setDataType(dataType);
		newCol.setNullable(isNullable);
		colInfoHash.put(colName, newCol);
	}
	
	public void setPri(String[] colList) throws SQLException{
		
		//Non-existence check
		for(int i=0; i<colList.length; i++) {
			if ( !this.contains(colList[i]) ) {
				throw new SQLException("Non Existing col");
			}
		}
		
		//Set KeyType of each columns
		priColList = Arrays.copyOf(colList, colList.length);
		
		for(int i=0; i<priColList.length; i++) {
			//original column info
			ColInfo nowCol = colInfoHash.get(priColList[i]); 
			
			//set KeyType to PRI, if already foreign key, then set BOTH
			KeyType kt = nowCol.getKeyType();
			switch (kt) {
				case FOR :
					nowCol.setKeyType(KeyType.BOTH);
					break;
				default : 
					nowCol.setKeyType(KeyType.PRI);
			}
			
			//primary key must not nullable
			nowCol.setNullable(false);
			
			//update colInfo
			colInfoHash.put(priColList[i], nowCol);
		}
		
	}
	
	public void setFor(String[] refingColList, TableInfo refedTable, String[] refedColList) 
			throws SQLException {
		
		//// ReferenceTableExistenceError is checked by upper layer
		
		if( 	//Reference type matching check ok
				refTest(refingColList, refedTable, refedColList )	) {
				
			ForKey newForKey = new ForKey();
			newForKey.setRefedTable(refedTable.getTableName());
			
			for(int i=0; i<refingColList.length; i++) {
				//Set reference relationship
				newForKey.setRef(refingColList[i], refedColList[i]);
			
				//original column info
				ColInfo nowCol = colInfoHash.get(refingColList[i]); 
				
				//Set KeyType of each referencing columns
				KeyType kt = nowCol.getKeyType();
				switch (kt) {
					case PRI :
						nowCol.setKeyType(KeyType.BOTH);
						break;
					default : 
						nowCol.setKeyType(KeyType.FOR);
				}
				
				//update colInfo
				colInfoHash.put(refingColList[i], nowCol);
			}
			
			forKeyList.add(newForKey);
			
		}
	}
	
	private boolean refTest(String[] refingColList, TableInfo refedTable, String[] refedColList) 
			throws SQLException{
		ColInfo refingCol;
		ColInfo refedCol; 
		
		for(int i=0; i<refingColList.length; i++) {
			if(!this.contains(refingColList[i]) ) {
				throw new SQLException("NonExistingColumnDefError");
			}
			refingCol = this.getColInfo(refingColList[i]);
			
			if(!refedTable.contains(refedColList[i]) ) {
				throw new SQLException("ReferenceColumnExistenceError");
			}
			refedCol = refedTable.getColInfo(refedColList[i]);
			
			if ( !refingCol.getDataType().equals(refedCol.getDataType() ) ) {
				throw new SQLException("ReferenceTypeError");
			} else if ( ( refedCol.getKeyType() != KeyType.PRI ) && 
						( refedCol.getKeyType() != KeyType.BOTH ) ) {
				throw new SQLException("ReferenceNonPrimaryKeyError");
			}
		}
		
		return true;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public ColInfo getColInfo(String colName) throws SQLException{
		if( !colInfoHash.containsKey(colName) ) {
			throw new SQLException("NonExistingCol");
		}
		return colInfoHash.get(colName);
	}
	
	public String[] getPriColList() {
		return priColList;
	}
	
	public String[] getForColList() {
		return forColList;
	}
	
	public boolean contains(String colName) {
		return colInfoHash.containsKey(colName);
	}
}


















class ColInfo{
	
	private String colName;
	private String dataType;
	private boolean nullable;
	private KeyType keyType;
	
	public ColInfo() {}
	
	public ColInfo(String name) {this.colName = name;}
	
	public void setColName(String colName) { this.colName = colName; }

	public void setDataType(String dataType) { this.dataType = dataType; }

	public void setNullable(boolean nullable) { this.nullable = nullable; }

	public void setKeyType(KeyType keyType) { this.keyType = keyType; }
	

	public String getColName() { return colName; }

	public String getDataType() { return dataType; }

	public boolean isNullable() { return nullable; }

	public KeyType getKeyType() { return keyType; }

	
}













class ForKey {
	
	private String refedTableName;
	private HashMap<String, String> matchingCol = new HashMap<String, String>();  
	
	public ForKey () {}

	public void setRefedTable(String name) {
		this. refedTableName = name;
	}
	
	public void setRef(String refingCol, String refedCol) throws SQLException {
		if( matchingCol.containsKey(refingCol) ) {
			throw new SQLException("Duplicated column in one reference key");
		} 
		
		matchingCol.put(refingCol, refedCol);
	}
	
	public String getRefedTable() {
		return refedTableName;
	}
	
	public String[] getRefedCols() {
		return (String[]) matchingCol.keySet().toArray();
	}
	
	public String[] getRefingCols() {
		return (String[]) matchingCol.values().toArray();
		
	}
	
	public String getMatchingCol(String refingCol) throws SQLException {
		if( !matchingCol.containsKey(refingCol) ) {
			throw new SQLException("No Existing Col");
		}
		return matchingCol.get(refingCol); 
	}
		
}





