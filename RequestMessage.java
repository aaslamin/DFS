import java.util.Locale;


public class RequestMessage {
	
	public enum RequestMethod {
		READ, 
		NEW_TXN, 
		WRITE, 
		COMMIT, 
		ABORT;
		
		public static RequestMethod fromString (String method) {
			if (method != null) {
				for (RequestMethod m : RequestMethod.values()) {
					if (method.equalsIgnoreCase(m.name())) return m;
				}
			}
			throw new IllegalArgumentException(String.format("Invalid method (%s) provided", method));
		}
	}
	
	private RequestMethod method;
	private int transactionID;
	private int sequenceNumber;
	private int contentLength;
	private String data;
	
	public RequestMessage (RequestMethod method, int transactionID, int sequenceNumber, int contentLength, String data) {
		this.method = method;
		this.transactionID = transactionID;
		this.sequenceNumber = sequenceNumber;
		this.contentLength = contentLength;
		this.data = data;
	}

	public RequestMethod getMethod() {
		return method;
	}

	public int getTransactionID() {
		return transactionID;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}

	public int getContentLength() {
		return contentLength;
	}

	public String getData() {
		return data;
	}
}
