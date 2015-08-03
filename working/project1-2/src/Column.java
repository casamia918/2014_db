import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

enum DataType {
	INT, CHAR, DATE;
}

enum KeyType {
	PRI, SEC, REF;
}

public class Column<T> implements Serializable {
	
	String colName;
	
	DataType dataType;
	ArrayList<T> dataArr = null;
	
	int length;
	
	boolean isPK = false; // this column is primary key
	boolean isSK = false; // this column is secondary key
	boolean isRK = false; // this column is reference key
	boolean nullable;
	
	public Column (String name, DataType dt, KeyType kt, boolean nullable ) 
	throws Exception {
	
		length = 0;
		colName = name;
		dataType = dt;
		dataArr = new ArrayList<T>();
		
		switch (kt) {
			case PRI :
				isPK = true;
				break;
			case SEC :
				isSK = true;
				break;
			case REF :
				isRK = true;
				break;
		}
		
		if(!isPK) {
			this.nullable = nullable;
		} else {
			if(nullable) {
				throw new Exception("PrimaryKeyNullableError" + colName);
			} else {
				this.nullable = false;	
			}
			
				
		}
		
		
	}

	
	

	public String getColName() {
		return colName;
	}


	public DataType getDataType() {
		return dataType;
	}


	public ArrayList<T> getData() {
		return dataArr;
	}


	public int getLength() {
		return length;
	}


	public boolean isPK() {
		return isPK;
	}


	public boolean isSK() {
		return isSK;
	}


	public boolean isRK() {
		return isRK;
	}


	public boolean isNullable() {
		return nullable;
	}


	
	
	
	
	
	public void setColName(String colName) {
		this.colName = colName;
	}


	public void setPK(boolean isPK) {
		this.isPK = isPK;
		this.nullable = false;
	}


	public void setSK(boolean isSK) {
		this.isSK = isSK;
	}


	public void setRK(boolean isRK) {
		this.isRK = isRK;
	}


	public void setNullable(boolean nullable) throws Exception {
		this.nullable = nullable;
		if(isPK & nullable ){
			throw new Exception("PrimaryKeyNullableError" + colName);
		}
	}
	
	
	
	
	
	
	
}

