import java.io.Serializable;

public class ServerMessage implements Serializable {
	
    private static final long serialVersionUID = 5950169519310163654L;
	private int commitLSN;
	private int transactionID;
	private ResponseMethod responseMethod;
	private RequestMethod requestMethod;
	
	public enum ResponseMethod {
		ASK_RESEND,
		ACK;
		
		public static ResponseMethod fromString (String method) {
			if (method != null) {
				for (ResponseMethod m : ResponseMethod.values()) {
					if (method.equalsIgnoreCase(m.name())) return m;
				}
			}
			throw new IllegalArgumentException(String.format("Invalid method (%s) provided", method));
		}
	}
	
	public enum RequestMethod {
		SYNC,
		COMMIT;
		
		public static RequestMethod fromString (String method) {
			if (method != null) {
				for (RequestMethod m : RequestMethod.values()) {
					if (method.equalsIgnoreCase(m.name())) return m;
				}
			}
			throw new IllegalArgumentException(String.format("Invalid method (%s) provided", method));
		}
	}
	
	public ServerMessage (RequestMethod request, int LSN) {
		requestMethod = request;
		commitLSN = LSN;
	}
	
	public ServerMessage (ResponseMethod response, int LSN) {
		responseMethod = response;
		commitLSN = LSN;
	}
	
	public RequestMethod getRequestMethod () {
		return requestMethod;
	}
	
	public ResponseMethod getResponseMethod () {
		return responseMethod;
	}
	
	public void setResponseMethod (ResponseMethod method) {
		responseMethod = method;
	}
	
	public int getTransactionID () {
		return transactionID;
	}
	
	public void setRequestMethod (RequestMethod method) {
		requestMethod = method;
	}
	
	public int getCommitLSN () {
		return commitLSN;
	}
}
