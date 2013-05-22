package kissmydisc.repricer.dao;

public class DBException extends Exception {
	
	private static final long serialVersionUID = 372587963185425379L;

	public DBException() {
		super();
	}

	public DBException(String message) {
		super(message);
	}

	public DBException(String message, Throwable t) {
		super(message, t);
	}

	public DBException(Throwable t) {
		super(t);
	}
}
