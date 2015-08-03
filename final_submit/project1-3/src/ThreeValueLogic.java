import java.util.Date;


enum ThreeValue {
		TRUE, FALSE, UNKNOWN
}
	
public class ThreeValueLogic{
	
	public ThreeValue and(ThreeValue a, ThreeValue b) {
		if( a == ThreeValue.TRUE && b == ThreeValue.TRUE ) {
			return ThreeValue.TRUE;
		} else if ( a == ThreeValue.FALSE || b == ThreeValue.FALSE) {
			return ThreeValue.FALSE;
		} else {
			return ThreeValue.UNKNOWN;
		}
	}
	
	public ThreeValue or(ThreeValue a, ThreeValue b) {
		if( a == ThreeValue.FALSE && b == ThreeValue.FALSE ) {
			return ThreeValue.FALSE;
		} else if ( a == ThreeValue.TRUE || b == ThreeValue.TRUE ) {
			return ThreeValue.TRUE;
		} else {
			return ThreeValue.UNKNOWN;
		}
	}
	
	public ThreeValue neg(ThreeValue a) {
		if( a == ThreeValue.FALSE ) {
			return ThreeValue.TRUE;
		} else if ( a == ThreeValue.TRUE) {
			return ThreeValue.FALSE;
		} else {
			return ThreeValue.UNKNOWN;
		}
	}
	

	
}
	