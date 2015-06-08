import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class ConcurrencyTest {

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

	private static InetAddress serverAddress = null;
	private static int serverPort;
	private static String filename; // the file that all clients will be writing to
	private static int clientCount; // number of clients to simulate
	private static Socket socket = null;

	private static final String[] requiredOptions = {"c", "ip", "port", "file"};
	private static final String CRLF = "\r\n"; 
	private static final int CRLF_CONTENT = 2; /* number of CRLF to use when there is content */
	private static final int CRLF_NO_CONTENT = 3; /* number of CRLF to use when there is no content */
	private static StringBuilder sb = new StringBuilder(); // will be storing the EXPECTED test result (transaction that commit will be appending their results to this)

	public static CountDownLatch endBarrier;

	public static void main (String[] args) {

		HashMap <String, String> options = null;
		try {
			options = getOptions (args, requiredOptions);
		} catch (IllegalArgumentException e) {
			System.err.println("Error: " + e.getMessage());
			printUsage();
			System.exit(1);
		}

		for (int i = 0; i < requiredOptions.length; i++) {
			if (!options.containsKey(requiredOptions[i])) {
				System.err.println("Error: -" + requiredOptions[i] + ": is a required option.");
				printUsage();
				System.exit(1);
			}
		}

		try {
			serverAddress = InetAddress.getByName(options.get("ip"));
			serverPort = new Integer(options.get("port")).intValue();
			filename = options.get("file");
			clientCount = new Integer(options.get("c")).intValue();
			socket = new Socket(serverAddress, serverPort);
			endBarrier = new CountDownLatch(clientCount);

			// if file already exists on the server, we keep track of its contents so our expected value is correct
			String fileData = readFileFromServer(filename);
			if (fileData != null) {
				System.out.println("FYI: the file you are writing to already exists on the server; writes will be appended to any existing content");
				System.out.println("Current length of (" + filename + ") is " + fileData.length() + " bytes");
				sb.append(fileData);
			}

			ArrayList<Integer> transactionList = requestTransactions(clientCount, filename);

			int data = 0;
			for (Integer i : transactionList) {
				new Client(serverAddress.getHostAddress(), serverPort, filename, Integer.toString(data), i.intValue()).start();
				data++;
			}

			endBarrier.await();
			System.out.println("EXPECTED result (" + sb.toString().length() + " bytes):");
			System.out.println(sb.toString());

			Thread.sleep(3000);
			fileData = readFileFromServer(filename);

			System.out.println("OBSERVED result (" + fileData.length() + " bytes):");
			System.out.println(fileData);


			System.out.println("Commit order - by transaction ID: ");
			for (Integer s : Client.commitOrder) {
				System.out.println(s);
			}
			System.out.println();
			boolean passed = sb.toString().equals(fileData);
			if (passed) {
				System.out.println("Server has PASSED the concurrency test");
			} else {
				System.out.println("Server has FAILED the concurrency test, file contents do not match the expected value");
			}

		} catch (NumberFormatException e) {
			System.err.println("Invalid option argument provided, -port and -c must be valid numbers");
			System.exit(1);
		} catch (UnknownHostException e) {
			System.err.println("Invalid server address: " + serverAddress);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("IOException: " + e.getMessage());
			System.exit(1);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	// transactions will append their data here and commit 
	public synchronized static void appendData (String data) {
		sb.append(data);
	}

	public static String getExpectedResult () {
		return sb.toString();
	}

	public static int getTotalClients () {
		return clientCount;
	}

	public static String readFileFromServer (String filename) {
		Socket s = null;
		ByteArrayOutputStream os = null;
		try {
			s = new Socket(serverAddress, serverPort);
			BufferedInputStream in = null;
			DataOutputStream out = null;

			try {
				in = new BufferedInputStream(s.getInputStream());
				out = new DataOutputStream(s.getOutputStream());
			} catch (IOException e) {
				System.err.println("IOException: " + e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}

			byte[] message = new byte [1];
			int bytesRead = 0;
			int contentLength;
			String endSequence = repeat(CRLF, 2);
			os = new ByteArrayOutputStream();
			boolean doneReading = false;
			String line = null;
			String[] header = null;

			String request = ConcurrencyTest.buildRequest(
					ConcurrencyTest.RequestMethod.READ, 
					-1, 
					-1, 
					filename.length(), 
					filename);

			try {
				out.writeBytes(request);
				out.flush();
			} catch (IOException e) {
				System.err.println("Failed to send read request to server: " + e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}

			try {
				while (!doneReading && ((bytesRead = in.read(message, 0, message.length)) != -1)) {

					os.write(message, 0, bytesRead);
					line = os.toString();

					if (line.contains(endSequence)) {
						//System.out.println(line);
						line = line.substring(0, line.indexOf(endSequence));
						header = line.split(" ");
						if (header[0].equalsIgnoreCase("ERROR")) {
							//System.out.println("File doesn't exist on server");
							return null;
						}
						break;
					}				
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Failed to read response from server: " + e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}

			// header has been parsed, read the data portion
			int totalBytesRead = 0;
			bytesRead = 0;
			contentLength = new Integer(header[4]);
			//System.out.println("Content length: " + contentLength);
			message = new byte[1];
			os.reset();

			try {
				while ((bytesRead = in.read(message, 0, message.length)) != -1) {
					totalBytesRead += bytesRead;
					os.write(message, 0, bytesRead);
					if (totalBytesRead == contentLength) break;
				}
			} catch (IOException e) {
				System.err.println("Failed to completely read data from the server: " + e.getMessage());
				System.exit(1);
			}

			if (totalBytesRead != contentLength) {
				System.err.println("Failed to completely read data from the server");
				System.exit(1);
			}
			
		} catch (IOException e) {
			System.err.println("Failed to connect to server: " + e.getMessage());
			System.exit(1);
		}
		finally {

			if (s != null) {
				try {
					Thread.currentThread().sleep(50);
					s.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return os.toString();
	}


	public static ArrayList<Integer> requestTransactions (int count, String filename) {
		Socket s = null;
		ArrayList<Integer> transactionList = null;
		try {
			s = new Socket(serverAddress, serverPort);
			s.setSoTimeout(200);
			BufferedInputStream in = null;
			DataOutputStream out = null;
			try {
				in = new BufferedInputStream(s.getInputStream());
				out = new DataOutputStream(s.getOutputStream());
			} catch (IOException e) {
				System.err.println("IOException: " + e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}

			transactionList = new ArrayList<Integer>();
			String newTransactionRequest = buildRequest(RequestMethod.NEW_TXN, -1, 0, filename.length(), filename);
			int bytesRead = 0;
			String responseHeader = null;
			byte[] buffer = new byte [1024];

			// will send [count] NEW_TXN requests to the server and create a list of the transaction IDs returned by the server

			for (int i = 0; i < count; i++) {
				try {
					out.writeBytes(newTransactionRequest);
					out.flush();
				} catch (IOException e) {
					System.err.println("Could not send request to server: " + e.getMessage());
					System.exit(1);
				}
				// read the transaction ID returned by the server
				bytesRead = 0;
				try {
					while ((bytesRead = in.read(buffer, 0, buffer.length)) != -1) {
						buffer[bytesRead] = (byte) '\0';
						responseHeader = new String (buffer);
						responseHeader = responseHeader.substring(0, responseHeader.indexOf(CRLF));
						//System.out.println(responseHeader);
						transactionList.add(new Integer(responseHeader.split(" ")[1]));
					}
				} catch (SocketTimeoutException e) {
					
				} catch (NumberFormatException e) {
					System.err.println("NumberFormatException: " + e.getMessage());
					System.exit(1);
				} catch (IOException e) {
					System.err.println("IOException: " + e.getMessage());
					System.exit(1);
				}
			}

			//		for (Integer i : TIDlist) {
			//			System.out.println(i);
			//		}

			
		} catch (IOException e) {
			System.err.println("Failed to connect to server: " + e.getMessage());
			System.exit(1);
		} 
		finally {
			if (s != null) {
				try {
					Thread.currentThread().sleep(50);
					s.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return transactionList;
	}



	public static final String getCRLF () {
		return CRLF;
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

	// Note: only request messages that should contain data are NEW_TXN, READ, & WRITE
	public static String buildRequest (RequestMethod method, int tid, int sequenceNumber, int contentLength, String data) {

		String response = method.name() + " " + tid + " " + sequenceNumber + " " + contentLength;
		if (contentLength == 0 && data == null)
			return response + repeat(CRLF, CRLF_NO_CONTENT);
		else
			return response + repeat(CRLF, CRLF_CONTENT) + data;
	}

	private static void printUsage () {
		System.out.println("Usage: ");
		System.out.println("java ConcurrencyTest -c [number_of_clients] -file [filename] -ip [ip_address_string] -port [port_number]");
		System.out.println("This utility program will create a number of clients that send a series of write requests to one file and then commits and will then check if your server has correctly serialized access to this file");
	}

	private static HashMap<String, String> getOptions (String[] args, String[] programOptions) throws IllegalArgumentException {
		HashMap<String, String> options = new HashMap<String, String>();

		for (int i = 0; i < args.length; i++) {
			final String opt = args[i];

			if (opt.charAt(0) == '-') {
				if (Arrays.asList(programOptions).contains(opt.substring(1).toLowerCase())) {
					i++;
					if (i < args.length) {
						if (!options.containsKey(opt.substring(1))) {
							//System.out.println("length of args: " + args.length + " value of i: " + i);
							options.put(opt.substring(1).toLowerCase(), args[i]);
						}
						else
							throw new IllegalArgumentException("Input error: option (" + opt + ") has already been declared");
					} else {
						throw new IllegalArgumentException("Input error: option argument for (" + opt + ") is missing");
					}	
				} else {
					throw new IllegalArgumentException("Input error: option (" + opt + ") is not a valid option");
				}	
			} else {
				throw new IllegalArgumentException("Input error: missing dash (-), command line options must be of the form -<option> <option argument>");
			}
		}
		return options;
	}


}
