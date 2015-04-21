import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ServerException extends Exception {
	
	ClientServerProtocol.Error type;
	
	public ServerException (String error, ClientServerProtocol.Error type) {
		super(error);
		this.type = type;
	}
	
	public ClientServerProtocol.Error getError () {
		return type;
	}
} 

public class ClientServerProtocol {
	
	private static final String CRLF = "\r\n"; 
	private static final int CRLF_CONTENT = 2; /* number of CRLF to use when there is content */
	private static final int CRLF_NO_CONTENT = 3; /* number of CRLF to use when there is no content */
	private static final int MAX_CONTENT_LENGTH = 2048; /* maximum length of content (bytes) accepted by the server */
//	private static final char[] ILLEGAL_CHARACTERS = { '/', '\n', '\r', '\t', '\0', '\f', '`', '?', '*', '\\', '<', '>', '|', '\"', ':' };

	
	public enum ResponseMethod {
		ACK, 
		ASK_RESEND, 
		ERROR
	}
	
	public enum Error {
		INVALID_TRANSACTION_ID (201, "Invalid transaction ID"), 
		INVALID_OPERATION (202, "Invalid operation"), 
		WRONG_MESSAGE_FORMAT (204, "Wrong message format"), 
		FILE_IO_ERROR (205, "File I/O error"), 
		FILE_NOT_FOUND (206, "File not found"),
		TIMEOUT (504, "Timeout"),
		NONE (0, "None");
		
		private final int id;
		private final String reason;
		
		Error (int id, String reason) {
			this.id = id;
			this.reason = reason;
		}
		
		public int getId() {
			return id;
		}
		
		@Override
		public String toString () {
			return reason;
		}
	}
	
	public static boolean containsIllegals(String toExamine) {
	    Pattern pattern = Pattern.compile("[^-_.A-Za-z0-9]");
	    Matcher matcher = pattern.matcher(toExamine);
	    return matcher.find();
	}	
	
	
	public static final String repeat (String s, int times) {
		if (s == null)
			return null;

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < times; i++) {
			sb.append(s);
		}
		return sb.toString();
	}
	
	public static final String getCRLF () {
		return CRLF;
	}
	
	public static String buildResponse (ResponseMethod method, int tid, int sequence, Error error, int contentLength, String content) {

		String response = method.name() + " " + tid + " " + sequence + " " + error.getId() + " " + contentLength;
		if (contentLength == 0 && content == null)
			return response + repeat(CRLF, CRLF_NO_CONTENT);
		else
			return response + repeat(CRLF, CRLF_CONTENT) + content;
	}
	
	public static RequestMessage parseMessage (DataInputStream in) throws ServerException, ServerException {	
		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		byte[] message = new byte[1];
		String line = null;
		boolean doneReading = false;
		boolean readHeader = false;
		int bytesRead = 0;
		int totalBytesRead = 0;
		String[] header = null;
		RequestMessage.RequestMethod method = null;
		int transactionID = -1;
		int sequenceNumber = -1;
		int contentLength= -1;
		String data = null;
		
		
		while (!doneReading) {
			
			try {
				// reading one byte at a time until a CRLF is encountered 
				while (!readHeader && ((bytesRead = in.read(message, 0, message.length)) != -1)) {
					
					
					/* read one byte at a time until a CRLF is encountered 
					 * the first CRLF indicates the end of the header portion of the message*/
					os.write(message, 0, bytesRead);

					line = os.toString();
					
					/* parsing the byte stream for the header portion of the request message */
					if (line.contains(CRLF)) {
						line = line.substring(0, line.indexOf(CRLF));
						line = line.replaceAll("\\n|\\r", "");
						header = line.split(" "); 
						if (header.length != 4) {
							throw new ServerException(String.format("Number of field(s) provided (%d), required (%d) ", header.length, 4), Error.WRONG_MESSAGE_FORMAT);
						}
						

						method = RequestMessage.RequestMethod.fromString(header[0]);
						sequenceNumber = Integer.parseInt(header[2]);
						contentLength = Integer.parseInt(header[3]);

//						if (sequenceNumber < 0) {
//							throw new ServerException("Invalid request header: sequence number cannot be negative");
//						}
						
//						if (method.name().equalsIgnoreCase("COMMIT")) {
//							System.out.println(header[1]);
//							//FileServer.addCommit(transactionID);
//						}

						if (contentLength < 0) {
							throw new ServerException("Content length cannot be negative", Error.WRONG_MESSAGE_FORMAT);
						}

						if (contentLength > MAX_CONTENT_LENGTH) {
							throw new ServerException("Content length is too large. The maximum size accepted is " + MAX_CONTENT_LENGTH + " bytes", Error.INVALID_OPERATION);
						}
						
						switch (method) {
						case NEW_TXN:

							if (sequenceNumber != 0) {
								throw new ServerException("Sequence number must be zero (0) for NEW_TXN", Error.WRONG_MESSAGE_FORMAT);
							}
							if (contentLength == 0) {
								throw new ServerException("Content length must be the length of the filename in bytes for NEW_TXN", Error.WRONG_MESSAGE_FORMAT);
							}
							break;
						case READ:
							transactionID = Integer.parseInt(header[1]);

							if (contentLength == 0) {
								throw new ServerException("Content length must be the length of the filename in bytes for READ", Error.WRONG_MESSAGE_FORMAT);
							}
							break;
						case WRITE:
							transactionID = Integer.parseInt(header[1]);

							if (sequenceNumber <= 0) {
								throw new ServerException("Sequence number must be > 0 for WRITE", Error.WRONG_MESSAGE_FORMAT);
							}
							break;
							
						case COMMIT:
							transactionID = Integer.parseInt(header[1]);
							
							if (sequenceNumber <= 0) {
								
								throw new ServerException("Sequence number must be > 0 for COMMIT", Error.WRONG_MESSAGE_FORMAT);
							}
							// fall through to default case if COMMIT, hence the lack of break
						default:
							
							/* When the method does not require data, check for correct CRLF syntax (2 CRLFs) 
							 * 
							 * Methods: COMMIT, ABORT
							 * 
							 * */
							
							// adjust to read byte at a time if necessary
							//System.out.println("got here");
							transactionID = Integer.parseInt(header[1]);

							
							totalBytesRead = 0;
							bytesRead = 0;
							message = new byte[repeat(CRLF,2).getBytes().length];
							os.reset();
							
							while ((bytesRead = in.read(message, 0, message.length)) != -1) {
								totalBytesRead += bytesRead;
								os.write(message, 0, bytesRead);
								if (totalBytesRead == message.length) break;
							}

							if (!os.toString().equals(repeat(CRLF, 2))) {
								throw new ServerException("Two CRLFs must be followed after the request header if there is no data", Error.WRONG_MESSAGE_FORMAT);
							} 
							
							if (method.name().equalsIgnoreCase("COMMIT")) {
								//System.out.println(transactionID);
								FileServer.addCommit(transactionID);
							}
							
							return new RequestMessage(method, transactionID, sequenceNumber, contentLength, null);
						}
						
						/* When the method has data, check for correct CRLF syntax (1 CRLF + data) 
						 * 
						 * Methods: NEW_TXN (filename), READ (filename), WRITE (data)
						 */
						
						
						totalBytesRead = 0;
						bytesRead = 0;
						message = new byte[1];
						os.reset();

						while ((bytesRead = in.read(message, 0, message.length)) != -1) {
							totalBytesRead += bytesRead;
							os.write(message, 0, bytesRead);
							
							if (totalBytesRead == 2) break;
						}
						
						if (!os.toString().equals(CRLF)) {
//							System.out.println("Error, value of totalBytesRead: " + totalBytesRead);
//							System.out.println("Error, value of message.length: " + message.length);
							throw new ServerException(os.toString() + ":" + method.name() + ":" + transactionID + ":" + sequenceNumber + " >> Expecting CRLF: this method type expects another CRLF after the header", Error.WRONG_MESSAGE_FORMAT);
						}
						
						readHeader = true;
					}
				}
				
				if (!readHeader) {
					throw new ServerException("Failed to read the request header, check syntax for CRLFs", Error.WRONG_MESSAGE_FORMAT);
				}
				
				/* header has been parsed, continue parsing data portion of the request message */
				
				totalBytesRead = 0;
				bytesRead = 0;
				message = new byte[contentLength];
				os.reset();

				while ((bytesRead = in.read(message, 0, message.length)) != -1) {
					totalBytesRead += bytesRead;
					os.write(message, 0, bytesRead);
					if (totalBytesRead == contentLength) break;
				}
				
				if (totalBytesRead != contentLength) {
					throw new ServerException(String.format("Data field is invalid, total bytes read (%d), does not match the specified content length (%d) ", totalBytesRead, contentLength), Error.WRONG_MESSAGE_FORMAT);
				}
				
				data = os.toString();
				
				// if there are special characters in the file name, report an error
				if (method.equals(RequestMessage.RequestMethod.NEW_TXN) || (method.equals(RequestMessage.RequestMethod.READ))) {
					if (data.trim().length() == 0 || data.startsWith(".") || containsIllegals(data) || data.length() > 254) {
						throw new ServerException("Invalid filename provided, filenames cannot contain spaces, invalid characters or be an empty string, and must be less than 254 characters", Error.WRONG_MESSAGE_FORMAT);
					}
				}
				
				doneReading = true;				

			} catch (SocketTimeoutException e) {
				throw new ServerException("Server timed out after " + FileServer.getTimeout() + "ms. Incomplete request message. " + e.getMessage(), Error.TIMEOUT);
			} catch (IOException e) {
				throw new ServerException("I/O error: " + e.getMessage(), Error.FILE_IO_ERROR);
			} catch (NumberFormatException e) {
				throw new ServerException("tid, sequence number, and length must be valid integers", Error.WRONG_MESSAGE_FORMAT);
			} catch (IllegalArgumentException e) {
				throw new ServerException(e.getMessage(), Error.WRONG_MESSAGE_FORMAT);
			}
		}
		
		return new RequestMessage (method, transactionID, sequenceNumber, contentLength, data);

	}
}
