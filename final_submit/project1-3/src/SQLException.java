public class SQLException extends Exception {

	private static final long serialVersionUID = 1L;

	public SQLException() {
		super();
	}

	public SQLException(String message) {
		super(message);
	}

}













// ////////////////////////////////////////////////////////////
//
//
// Create table error
//
//
// ////////////////////////////////////////////////////////////

class DuplicateColumnDefError extends SQLException {

	private static final long serialVersionUID = -5678815471354082408L;
	private static String msg = "Create table has failed : column definition is duplicated";
	public DuplicateColumnDefError() {
		super(msg);
	}

}

class DuplicatePrimaryKeyDefError extends SQLException {

	private static final long serialVersionUID = 136732836938294357L;
	private static String msg = "Create table has failed : primary key definition is duplicated";
	public DuplicatePrimaryKeyDefError() {
		super(msg);
	}

}

class ReferenceTypeError extends SQLException {

	private static final long serialVersionUID = -6990525281563262304L;
	private static String msg = "Create table has failed : foreign key references wrong type";
	public ReferenceTypeError() {
		super(msg);
	}

}

class ReferenceNonPrimaryKeyError extends SQLException {

	private static final long serialVersionUID = -3338534732820885771L;
	private static String msg = "Create table has failed : foreign key references non primary key column";
	public ReferenceNonPrimaryKeyError() {
		super(msg);
	}

}

class ReferenceColumnExistenceError extends SQLException {

	private static final long serialVersionUID = -1519858864521057291L;
	private static String msg = "Create table has failed : foreign key references non existing column";
	public ReferenceColumnExistenceError() {
		super(msg);
	}

}

class ReferenceTableExistenceError extends SQLException {

	private static final long serialVersionUID = -6953116239666220732L;
	private static String msg = "Create table has failed : foreign key references non existing table";
	public ReferenceTableExistenceError() {
		super(msg);
	}

}

class SelfReferencingError extends SQLException {

	private static final long serialVersionUID = 2846939967920335047L;
	private static String msg = "Create table has failed : foreign key references itself table";
	public SelfReferencingError() {
		super(msg);
	}

}

class DuplicatedReferenceTableError extends SQLException {
	
	private static final long serialVersionUID = 2100773246808397352L;
	private static String msg = "Create table has failed : foreign key references already referencing table";
	public DuplicatedReferenceTableError() {
		super(msg);
	}
}

class NonExistingColumnDefError extends SQLException {

	private static final long serialVersionUID = -792485335048734685L;
	private static String msg = "Create table has failed : '%s' does not exists in column definition";
	public NonExistingColumnDefError(String colName) {
		super(String.format(msg, colName));
	}

}

class TableExistenceError extends SQLException {

	private static final long serialVersionUID = -4029016694220646072L;
	private static String msg = "Create table has failed : table with the same name already exists";
	public TableExistenceError() {
		super(msg);
	}

}















// ////////////////////////////////////////////////////////////
//
//
// Insertion Error
//
//
// ////////////////////////////////////////////////////////////

class InsertDuplicatePrimaryKeyError extends SQLException {

	private static final long serialVersionUID = -3849407873557003775L;
	private static String msg = "Insertion has failed : Primary key duplication";
	public InsertDuplicatePrimaryKeyError() {
		super(msg);
	}

}

class InsertReferentialIntegrityError extends SQLException {

	private static final long serialVersionUID = 4216419521920081673L;
	private static String msg = "Insertion has failed : Referential integrity violation";
	public InsertReferentialIntegrityError() {
		super(msg);
	}

}

class InsertTypeMismatchError extends SQLException {

	private static final long serialVersionUID = -6703788609401584565L;
	private static String msg = "Insertion has failed : Types are not matched";
	public InsertTypeMismatchError() {
		super(msg);
	}

}

class InsertColumnExistenceError extends SQLException {

	private static final long serialVersionUID = -2290119797616082241L;
	private static String msg = "Insertion has faild : '%s' does not exist";
	public InsertColumnExistenceError(String colName) {
		super(String.format(msg, colName));
	}

}

class InsertColumnNonNullableError extends SQLException {

	private static final long serialVersionUID = 1565277389331635717L;
	private static String msg = "Insertion has faild : %s is not nullable";
	public InsertColumnNonNullableError(String colName) {
		super(String.format(msg, colName));
	}

}














// ////////////////////////////////////////////////////////////
//
//
// Delete error
//
//
// ////////////////////////////////////////////////////////////

class DeleteReferentialIntegrityPassed extends SQLException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6252877894729732830L;
	private static String msg = "'%d' row(s) are not deleted due to referential integrity";
	public DeleteReferentialIntegrityPassed(int count) {
		super(String.format(msg, count));
	}

}


class DeleteMultipleTableRefered extends SQLException {

	private static final long serialVersionUID = -715331730439156428L;
	/**
	 * 
	 */
	private static String msg = "From clause in delete query can only have 1 table";
	public DeleteMultipleTableRefered() {
		super(msg);
	}

}















// ////////////////////////////////////////////////////////////
//
//
// Selection error
//
//
// ////////////////////////////////////////////////////////////

class SelectTableExistenceError extends SQLException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3355255402742161118L;
	private static String msg = "Selection has failed : '%s' does not exist";
	public SelectTableExistenceError(String tabName) {
		super(String.format(msg, tabName));
	}

}

class SelectColumnResolveError extends SQLException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9093218988713507243L;
	private static String msg = "Selection has failed : fail to resolve '%s'";
	public SelectColumnResolveError(String tabName) {
		super(String.format(msg, tabName));
	}

}

class SelectNonComparableTypesError extends SQLException {

	private static final long serialVersionUID = -6703788609401584565L;
	private static String msg = "Where condition error : types are not matched in comparison";
	public SelectNonComparableTypesError() {
		super(msg);
	}

}

class SelectDuplicateAliasingError extends SQLException {

	private static final long serialVersionUID = -6703788609401584565L;
	private static String msg = "Alias is duplicated";
	public SelectDuplicateAliasingError() {
		super(msg);
	}

}

















// ////////////////////////////////////////////////////////////
//
//
// Drop error
//
//
// ////////////////////////////////////////////////////////////

class DropReferencedTableError extends SQLException {

	private static final long serialVersionUID = -6187812438382936900L;
	private static String msg = "Drop table has failed : '%s' is referenced by other table";
	public DropReferencedTableError(String tabName) {
		super(String.format(msg, tabName));
	}

}

















// ////////////////////////////////////////////////////////////
//
//
// Where clause error
//
//
// ////////////////////////////////////////////////////////////

class WhereIncomparableError extends SQLException {

	private static final long serialVersionUID = -1882450200192163918L;
	private static String msg = "Where clause try to compare incomparable values";
	public WhereIncomparableError() {
		super(msg);
	}

}

class WhereTableNotSpecified extends SQLException {

	private static final long serialVersionUID = -5600701477906796431L;
	private static String msg = "Where clause try to reference tables which are not specified ";
	public WhereTableNotSpecified() {
		super(msg);
	}

}

class WhereColumnNotExist extends SQLException {

	private static final long serialVersionUID = 2056916758435665658L;
	private static String msg = "Where clause try to reference non existing column";
	public WhereColumnNotExist() {
		super(msg);
	}

}

class WhereAmbiguousReference extends SQLException {

	private static final long serialVersionUID = -5880378861123303372L;
	private static String msg = "Where clause contains ambiguous reference";
	public WhereAmbiguousReference() {
		super(msg);
	}

}


















// ////////////////////////////////////////////////////////////
//
//
// Other error
//
//
// ////////////////////////////////////////////////////////////

class NoSuchTable extends SQLException {

	private static final long serialVersionUID = -4415374258074888525L;
	private static String msg = "No such table";
	public NoSuchTable() {
		super(msg);
	}

}

class ShowTablesNoTable extends SQLException {

	private static final long serialVersionUID = -7597764754557278634L;
	private static String msg = "There is no table";
	public ShowTablesNoTable() {
		super(msg);
	}

}

class InvalidDateRangeError extends SQLException {

	private static final long serialVersionUID = 7237653432977475269L;
	private static String msg = "Date value is in incorrect range";
	public InvalidDateRangeError() {
		super(msg);
	}

}

class CharLengthError extends SQLException {

	private static final long serialVersionUID = -8395976940024404151L;
	private static String msg = "Char length should be > 0";
	public CharLengthError() {
		super(msg);
	}

}
