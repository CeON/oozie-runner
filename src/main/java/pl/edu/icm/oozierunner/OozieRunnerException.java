package pl.edu.icm.oozierunner;

public class OozieRunnerException extends RuntimeException {

	private static final long serialVersionUID = 4942084546686572968L;

	public OozieRunnerException() {
		super();
	}

        /*
         *
	public OozieRunnerException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
        * 
        */

	public OozieRunnerException(String message, Throwable cause) {
		super(message, cause);
	}

	public OozieRunnerException(String message) {
		super(message);
	}

	public OozieRunnerException(Throwable cause) {
		super(cause);
	}
}
