interface MyConstants {
	

	public static final boolean NOT_READONLY = false;
	public static final boolean READONLY = true;
	
	public static final int COLNAME = 0;
	public static final int DATATYPE = 1;
	public static final int NULLITY = 2;
	public static final int KEYTYPE = 3;
	
	public static final int FORCOL = 0;
	public static final int REFEDTABLENAME = 1;
	public static final int REFEDCOL = 2;
	
	public static final int SUBJECT = 1;
	public static final int OBJECT_TABLENAME = 2;
	public static final int OBJECT = 3;
	
	public static final int INT_VAL = 0;
	public static final int CHAR_STR = 1;
	public static final int DATE_VAL = 2;
	public static final int COL_REF = 3;
	
	public static final int QUERY_SEL = 0;
	public static final int QUERY_DEL = 1;
	
	//synchronize with token disclaimer in SQLParser.jj
	public static final String TOK_OR = "or\""; 
	public static final String TOK_AND = "and\"";
	public static final String TOK_NOT = "not\"";
	public static final String TOK_IS = "is\"";
	public static final String TOK_LEFTPAR = "(\"";
	public static final String TOK_RIGHTPAR = ")\"";
	public static final String TOK_ISNOTNULL = "is not null\"";
	public static final String TOK_ISNULL = "is null\"";
	
	public static final String PATTERNSTR_DATE = "(\\d{1,4})(-)(\\d{1,2})(-)(\\d{1,2})";
	public static final String PATTERNSTR_INT = "(-)?(\\d)+";
	public static final String PATTERNSTR_CHAR = "\'([^\"\'\t\n\r])*\'";
	public static final String PATTERNSTR_COMPOP = " < | > | = | >= | <= | !="; 
	
	public static final String PREFIX_VAL = "\"VAL\"";
	public static final String PREFIX_CR = "\"CR\"";
	
	public static final String COLDEF_PREFIX = "@cd";
	public static final String FORKEY_PREFIX = "#fk";
	public static final String PRIKEY_PREFIX = "*pk";
	public static final String TUPLE_PREFIX = "-tp";
	public static final String REFINGPT_PREFIX = "!ref->";
	public static final String REFEDPT_PREFIX = "!ref<-"; 
	
	
}