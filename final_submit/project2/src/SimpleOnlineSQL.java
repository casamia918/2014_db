import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SimpleOnlineSQL {
		static SimpleOnlineSQL mySOS;
		static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		static String driverName = "oracle.jdbc.driver.OracleDriver"; 
		static String serverName = "***.***.***.***";
		static String portNumber = "****";
		static String sid = "****";
		static String url = "********"+ serverName + ":" + portNumber + ":" + sid; 
		static String username = "DB-*************";
		static String password = "DB-********";
		static boolean rightInput;

	public static void main(String[] args) throws Exception {
		mySOS = new SimpleOnlineSQL(); 
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection( url, username, password );
	
		while(true) {
			try {
				if( mySOS.runSQL(conn) ) {
					break;
				}
			} catch ( Exception e) {
				System.out.println(e.getMessage());
				//e.printStackTrace();
				System.out.println();
			}
		}
		System.out.println();
		System.out.println("Thanks!");
		
		
	}
	
	public boolean runSQL(Connection conn) throws Exception { 

		mySOS.menu();
		int listNum = 0;
		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Select your action: ");
				listNum = Integer.parseInt(br.readLine());
				rightInput = true;
			} catch ( NumberFormatException nfe) {
				System.out.println("WRONG_INPUTTYPE : Wrong input type");
				rightInput = false;
			} 
		}
		System.out.println();
		
		rightInput = false;
		while(!rightInput) {
			try {
				if(listNum == 10) {
					return true;
				} else {
					switch(listNum) {
						case 1 :
							mySOS.listAllLectures(conn);
							break;
						case 2 :
							mySOS.listAllStudents(conn);
							break;
						case 3 :
							mySOS.insertLecture(conn);
							break;
						case 4 :
							mySOS.removeLecture(conn);
							break;
						case 5 :
							mySOS.insertStudent(conn);
							break;
						case 6 :
							mySOS.removeStudent(conn);
							break;
						case 7 :
							mySOS.registerLecture(conn);
							break;
						case 8 :
							mySOS.listAllLectureOfStudent(conn);
							break;
						case 9 :
							mySOS.listAllRegesteredStudentOfLecture(conn);
							break;
						default :
							System.out.println("WRONG_SELECTION : Please select 1~10");
						break;
					}
					rightInput = true;
				}
			} catch (SQLException se) {
				System.out.println(se.getMessage());
				System.out.println();
				rightInput = false;
			}
		}
		
		
		return false;
	}
	
	public void menu() {
		System.out.println("====================");
		System.out.println("1. list all lectures");
		System.out.println("2. list all students");
		System.out.println("3. insert a lecture");
		System.out.println("4. remove a lecture");
		System.out.println("5. insert a student");
		System.out.println("6. remove a student");
		System.out.println("7. register for lecture");
		System.out.println("8. list all lectures of a student");
		System.out.println("9. list all registered students of a lecture"); 
		System.out.println("10. exit");
		System.out.println("===================");
	}
	
	
	public void listAllLectures(Connection _conn) throws SQLException {

		String query = 
				"select * "
				+ "from lecture "
				+ "natural left outer join (select lecture_id as id , count(student_id) as Current_Applied from registration group by (lecture_id)) "
				+ "order by (id)";

		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>(); 
		
		ArrayList<String> colList = new ArrayList<String>();
		colList.add("Id"); colList.add("Name"); colList.add("Credit"); 
		colList.add("Capacity"); colList.add("Current Applied");
		
		ArrayList<String> nowTuple = null;
		
		PreparedStatement stmt = _conn.prepareStatement(query);
		ResultSet rs = stmt.executeQuery();

		while(rs.next() ) {
			nowTuple = new ArrayList<String>();
			
			int id = rs.getInt("id");
			nowTuple.add(String.valueOf(id));
			
			String name = rs.getString("name");
			nowTuple.add(name);
			
			int credit =rs.getInt("credit");
			nowTuple.add(String.valueOf(credit));
			
			int capacity = rs.getInt("capacity");
			nowTuple.add(String.valueOf(capacity));

			int currentApplied = rs.getInt("Current_Applied");
			nowTuple.add(String.valueOf(currentApplied));
			
			result.add(nowTuple);
		}
		stmt.close();
		mySOS.print(colList, result);
		
		
		
	}
	

	public void listAllStudents(Connection _conn) throws SQLException {

		String query = 
				"select id, name, sum(credit) as used_credits "
				+ "from student "
				+ "natural left outer join (select student_id as id, lecture_id from registration) "
				+ "natural left outer join (select id as lecture_id, credit from lecture) "
				+ "group by (id, name)";

		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>(); 
		
		ArrayList<String> colList = new ArrayList<String>();
		colList.add("Id"); colList.add("Name"); colList.add("Used Credits"); 
		
		ArrayList<String> nowTuple = null;
		
		PreparedStatement stmt = _conn.prepareStatement(query);
		ResultSet rs = stmt.executeQuery();

		while(rs.next() ) {
			nowTuple = new ArrayList<String>();
			
			String id = rs.getString("id");
			nowTuple.add(String.valueOf(id));
			
			String name = rs.getString("name");
			nowTuple.add(name);
			
			int used_credit =rs.getInt("used_credits");
			nowTuple.add(String.valueOf(used_credit));
			
			result.add(nowTuple);
		}
		stmt.close();
		mySOS.print(colList, result);
		
		
	}
	
	
	public void insertLecture(Connection _conn ) throws SQLException, Exception {
		String lectureName = null ;
		int credit = 0 ;
		int capacity = 0 ;
		
		System.out.print("Input lecture name: ");
		lectureName = br.readLine();
		
		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input lecture credit: ");
				credit = Integer.parseInt( br.readLine() );
				if( credit <= 0 ) {
					throw new SQLException("INSERT_LECERR_CREDIT : Credit should be over 0.");
				} 
				rightInput = true;
			} catch (NumberFormatException nfe) {
				System.out.println("WRONG_INPUTTYPE : Wrong input type");
				rightInput = false;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				rightInput = false;
			}
		}		
		
 		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input lecture capacity: ");
				capacity = Integer.parseInt( br.readLine() );
		 		if(capacity <= 0) {
					throw new SQLException("INSERT_LECERR_CAPACITY : Capacity should be over 0.");
				}
				rightInput = true;
			} catch (NumberFormatException nfe) {
				System.out.println("WRONG_INPUTTYPE : Wrong input type");
				rightInput = false;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				rightInput = false;
			}
		}		
		
 		
		System.out.println();
		
		String query = "INSERT INTO lecture values ( LECTURE_SEQ.NEXTVAL, ?, ?, ? )";
		PreparedStatement stmt = _conn.prepareStatement( query );
		stmt.setString( 1, lectureName); 
		stmt.setInt( 2, credit ); 
		stmt.setInt( 3, capacity );
		int rowCount = stmt.executeUpdate( );
		stmt.close();
		
		if(rowCount != 1) {
			throw new SQLException("Insert lecture failure");
		} else {
			System.out.println("INSERT_SUCCESS : A Lecture is successfully inserted.");
		}
		
		return;
	}

	
	public void removeLecture(Connection _conn) throws SQLException, Exception {
		String query = "delete from lecture where id = ?";
		
		int id  = 0;
		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input lecture id: ");
				id = Integer.parseInt( br.readLine() );
				rightInput = true;
			} catch (NumberFormatException nfe) {
				System.out.println("WRONG_INPUTTYPE : Wrong input type");
				rightInput = false;
			}
		}		
		
		System.out.println();
		
		PreparedStatement stmt = _conn.prepareStatement(query);
		stmt.setInt(1, id);
		int rowCount = stmt.executeUpdate();
		stmt.close();
		
		if (rowCount < 1 ) {
			throw new SQLException("Lecture "+ id +" doesn't exist");
		} else {
			System.out.println("DELETE_SUCCESS : A Lecture is successfully deleted.");
		}
	}
	
	
	public void insertStudent(Connection _conn) throws SQLException, IOException {
		String studentName = null;
		String id = null;
		
		System.out.print("Input student name: ");
		studentName = br.readLine();
		
 		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input student id: ");
				id = br.readLine();
				if( !isValidStudentId(id) ) {
					throw new SQLException("INSERT_STUERR_FORMAT : Id should have form 'nnnn-nnnnn'.");
				}
				rightInput = true;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				rightInput = false;
			}
		}		
		
		
		
		System.out.println();
		
		String query = "INSERT INTO student values ( ?, ? )";
		PreparedStatement stmt = _conn.prepareStatement( query );
		stmt.setString( 1, id); 
		stmt.setString( 2, studentName); 
		int rowCount = stmt.executeUpdate( );
		stmt.close();
		
		if(rowCount != 1) {
			throw new SQLException("Insert student failure");
		} else {
			System.out.println("INSERT_SUCCESS : A Student is successfully inserted.");
		}
		
		return;
		
	}
	

	
	public void removeStudent(Connection _conn) throws SQLException, Exception {
		String id = null;
		String query = "delete from student where id = ?";
		
 		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input student id: ");
				id = br.readLine();
				if( !isValidStudentId(id) ) {
					throw new SQLException("INSERT_STUERR_FORMAT : Id should have form 'nnnn-nnnnn'.");
				}
				rightInput = true;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				rightInput = false;
			}
		}		
		
		System.out.println();
		
		PreparedStatement stmt = _conn.prepareStatement(query);
		stmt.setString(1, id);
		int rowCount = stmt.executeUpdate();
		stmt.close();
		
		if (rowCount < 1 ) {
			throw new SQLException("Student "+ id +" doesn't exist");
		} else {
			System.out.println("DELETE_SUCCESS : A Student is successfully deleted.");
		}
	}
	
	

	public void registerLecture(Connection _conn) throws SQLException, Exception {
		int lecId = 0;
		String stuId = null;

		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input student id: ");
				stuId = br.readLine();
				if( !isValidStudentId(stuId) ) {
					throw new SQLException("INSERT_STUERR_FORMAT : Id should have form 'nnnn-nnnnn'.");
				}
				rightInput = true;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				rightInput = false;
			}
		}		
		
		
		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input lecture id: ");
				lecId = Integer.parseInt( br.readLine() );
				rightInput = true;
			} catch (NumberFormatException nfe) {
				System.out.println("WRONG_INPUTTYPE : Wrong input type");
				rightInput = false;
			}
		}	
		
		System.out.println();
		
		PreparedStatement selectStudent = null;
		PreparedStatement selectLecture = null;
		PreparedStatement updateRegister = null;
		
		String selectStudentQuery = 
				"select id, name, sum(credit) as used_credits "
				+ "from student "
				+ "natural left outer join (select student_id as id, lecture_id from registration) "
				+ "natural left outer join (select id as lecture_id, credit from lecture) "
				+ "group by (id, name)";
		String selectLectureQuery = 
				"select * "
				+ "from lecture "
				+ "natural left outer join (select lecture_id as id , count(student_id) as Current_Applied from registration group by (lecture_id) ) "
				+ "order by (id)";
		String updateQuery = "insert into registration values(?,?) ";
		
		try {
			// set auto commit to false to create transaction querying 
			_conn.setAutoCommit(false);
			selectStudent = _conn.prepareStatement(selectStudentQuery);
			selectLecture = _conn.prepareStatement(selectLectureQuery);
			updateRegister = _conn.prepareStatement(updateQuery);
				
			// read registered lecture information
			ResultSet rsLecture = selectLecture.executeQuery();
			int thisCredit = 0;
			int thisCapacity = 0;
			int thisApplied = 0;
			boolean lecOccur = false;
			while(rsLecture.next()) {
				int id = rsLecture.getInt("id");
				if(lecId == id) {
					thisCredit = rsLecture.getInt("credit");
					thisCapacity = rsLecture.getInt("capacity");
					thisApplied = rsLecture.getInt("current_applied");
					lecOccur = true;
					break;
				}
			}

			// read registering student information
			ResultSet rsStudent = selectStudent.executeQuery();
			int thisUsedCredit = 0;
			boolean stuOccur = false;
			while(rsStudent.next() ) {
				String id = rsStudent.getString("id");
				if(stuId.equalsIgnoreCase(id)) {
					thisUsedCredit = rsStudent.getInt("used_credits");
					stuOccur = true;
					break;
				}
			}

			if(!stuOccur ) {
				throw new SQLException("Student " + stuId + " doesn't exist");
			} else if(!lecOccur) {
				throw new SQLException("Lecture " + lecId + " doesn't exist");
			} else if( thisCapacity <= thisApplied) {
				throw new SQLException("INSERT_REGISTRERR_CAPACITY : Capacity of a lecture is full.");
			} else if ( thisUsedCredit + thisCredit > 18) {
				throw new SQLException("INSERT_REGISTRERR_CREDIT : No remaining credits.");
			} 
			
			
			updateRegister.setInt(1, lecId);
			updateRegister.setString(2, stuId);
			updateRegister.executeUpdate();
			
			System.out.println("INSERT_REGISTR_SUCCESS : Applied.");
			
			_conn.commit();
			
		} catch (SQLException se ) {
			if(_conn != null) {
                _conn.rollback();
				throw se;
			}
			
		} finally {
			if (selectStudent != null) {
				selectStudent.close();
	        }
			if (selectLecture != null) {
				selectLecture.close();
			}
	        if (updateRegister != null) {
	        	updateRegister.close();
	        }
	        _conn.setAutoCommit(true);
		}
		
		return;
		
	}
	
	

	public void listAllLectureOfStudent(Connection _conn) throws SQLException, Exception {

		String query = "select * from lecture "
				+ "natural left outer join (select lecture_id as id , count(student_id) as Current_Applied from registration group by (lecture_id)) "
				+ "where id in (select lecture_id from registration where student_id = ? ) "
				+ "order by (id) " ;
		
		String stuId = null;
		
 		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input student id: ");
				stuId = br.readLine();
				if( !isValidStudentId(stuId) ) {
					throw new SQLException("INSERT_STUERR_FORMAT : Id should have form 'nnnn-nnnnn'.");
				} else if ( !hasValue(_conn, "STUDENT", "ID", stuId) ) {
					throw new SQLException("Student " + stuId + " doesn't exist");
				}
				rightInput = true;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				rightInput = false;
			}
		}		
		
		System.out.println();

		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>(); 
		
		ArrayList<String> colList = new ArrayList<String>();
		colList.add("Id"); colList.add("Name"); colList.add("Credit"); 
		colList.add("Capacity"); colList.add("Current Applied");
		
		ArrayList<String> nowTuple = null;
		
		PreparedStatement stmt = _conn.prepareStatement(query);
		stmt.setString(1, stuId);
		ResultSet rs = stmt.executeQuery();

		while(rs.next() ) {
			nowTuple = new ArrayList<String>();
			
			int id = rs.getInt("id");
			nowTuple.add(String.valueOf(id));
			
			String name = rs.getString("name");
			nowTuple.add(name);
			
			int credit =rs.getInt("credit");
			nowTuple.add(String.valueOf(credit));
			
			int capacity = rs.getInt("capacity");
			nowTuple.add(String.valueOf(capacity));

			int currentApplied = rs.getInt("Current_Applied");
			nowTuple.add(String.valueOf(currentApplied));
			
			result.add(nowTuple);
		}
		stmt.close();
		mySOS.print(colList, result);
		
		
	}
	
	public void listAllRegesteredStudentOfLecture(Connection _conn) throws SQLException, Exception {

		String query = 
				"select * from student "
				+ "where id in (select student_id as id from registration where lecture_id = ?) ";
		
		int lecId = 0;
		
		rightInput = false;
		while(!rightInput) {
			try {
				System.out.print("Input lecture id: ");
				lecId = Integer.parseInt( br.readLine() );
				if ( !hasValue(_conn, "LECTURE", "ID", String.valueOf(lecId) ) ) {
					throw new SQLException("Lecture " + lecId + " doesn't exist");
				}
				rightInput = true;
			} catch (NumberFormatException nfe) {
				System.out.println("WRONG_INPUTTYPE : Wrong input type");
				rightInput = false;
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				rightInput = false;
			}
		}	
		
		System.out.println();
		
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>(); 
		
		ArrayList<String> colList = new ArrayList<String>();
		colList.add("Id"); colList.add("Name"); 
		
		ArrayList<String> nowTuple = null;
		
		PreparedStatement stmt = _conn.prepareStatement(query);
		stmt.setInt(1, lecId);
		ResultSet rs = stmt.executeQuery();

		while(rs.next() ) {
			nowTuple = new ArrayList<String>();
			
			String id = rs.getString("id");
			nowTuple.add(id);
			
			String name = rs.getString("name");
			nowTuple.add(name);
			
			result.add(nowTuple);
		}
		stmt.close();
		mySOS.print(colList, result);
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	

	
	
	
	
	
	public void print(ArrayList<String> colList, ArrayList<ArrayList<String>> tupleList) {
		
		// read maximum width from argument
		ArrayList<Integer> width = new ArrayList<Integer>();
		for(String nowCol : colList){
			width.add(nowCol.length());
		}
		for(ArrayList<String> nowTuple : tupleList) {
			for(int i=0; i<nowTuple.size(); i++) {
				if(nowTuple.get(i).length() > width.get(i)) {
					width.remove(i);
					width.add(i, nowTuple.get(i).length());
				}
			}
		}
		
		// add padding space
		for(int i=0; i<width.size(); i++) {
			int nowWidth = width.get(i);
			width.remove(i);
			width.add(i, nowWidth + 3); 
		}
		
		
		printDelimiterLine(width);
		printRecord(width, colList);
		printDelimiterLine(width);
		
		for(ArrayList<String> nowTuple : tupleList) {
			
			printRecord(width, nowTuple);
			
		}
		printDelimiterLine(width);
		
	}
	

	public void printDelimiterLine (ArrayList<Integer> _width) {
		String dash = new String();
		for(int nowWidth : _width) {
			for(int i=0; i<nowWidth; i++) {
				dash = dash + "-";
			}
		}
		System.out.println(dash);
	}
	
	public void printRecord(ArrayList<Integer> _width, ArrayList<String> _itemList) {
		String record = new String();
		for(int i=0; i<_width.size(); i++) {
			int nowWidth = _width.get(i);
			String nowItem = _itemList.get(i);
			record = record + String.format("%-"+nowWidth+"s", nowItem);
		}
		System.out.println(record);
	}
	
	public boolean isValidStudentId(String id) {

		Pattern idPattern = Pattern.compile("(\\d{4})(-)(\\d{5})");
		Matcher m = idPattern.matcher(id);
		if(m.matches()) {
			return true;
		}
		
		return false;
		
	}
	
	public boolean hasValue(Connection _conn, String _table, String _col, String _val) throws SQLException {
		

		String query = 
				"select count(*) as count "
				+ "from " + _table + " " 
				+ "where " + _col + " = ?";

		PreparedStatement stmt = _conn.prepareStatement(query);
		stmt.setString(1, _val);
		
		ResultSet rs = stmt.executeQuery();
		rs.next();
		int count = rs.getInt("count");
		stmt.close();
		
		if(count < 1) {
			return false;
		} else {
			return true;
		}
		
	}
	
	
}
