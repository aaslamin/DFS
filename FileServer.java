import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;

import javax.xml.bind.DatatypeConverter;

public class FileServer {
	
	// transaction log database - maintains a runtime data structure for ALL transactions
	public static HashMap <Integer, LogRecord> transactionDB = new HashMap<Integer, LogRecord>();
	// key: commitLSN, value: transaction records
	public static HashMap <Integer, LogRecord> commitDB = new HashMap<Integer, LogRecord>();
	
	// used to keep track of read/write accesses on every file present in the system
	private static HashMap <String, Integer> fileAccessManager = new HashMap<String, Integer>();
	public static Object mutexLock = new Object();
	private static Lock commitLock = new ReentrantLock(true);
	private static Lock commitQueue = new ReentrantLock(true);
	private static Object logLock = new Object();
	
	private final static Lock mutex = new ReentrantLock();
	private final static Condition removeCommitCond = mutex.newCondition();
	private final static Condition addCommitCond = commitQueue.newCondition();

	
	private static boolean commitInProgress = false;
	private static LinkedHashMap<Integer, Object> commitOrder = new LinkedHashMap<Integer, Object>();
	
	// flag that is used to know when to use the recoveryLogFile
	private static boolean inRecoveryMode = true; 

	private static final String logName = ".serverLog";
	private static final String recoveryLogName = "serverRecoveryLog";
	// this is a file prefix that is used to distinguish all files that have been created by the server for file keeping purposes
	private static final String tempFilePrefix = ".aqi1393029";
	// command line options
	private static final String[] commandLineOptions = {"dir", "ip", "port", "primary", "bip", "bport"};

	public static String primaryFile = null; // refers to the path of the file that contains the address of the primary server
	private static boolean isPrimary = false; // is this server the primary? 
	private static InetSocketAddress peerServer = null; // refers to the other server (primary/backup) depending on your current role
	private static HashMap<InetSocketAddress, Boolean> peerStatus = new HashMap<InetSocketAddress, Boolean>();
	private static File logFile = null;
	private static File recoveryLogFile = null;	

	private static String dir = null; // refers to the directory where the server will store files (i.e. logs, transactions, & metadata)
	public static int port;		
	public static InetAddress bindAddr = null;
	private static ServerSocket serverSocket = null;
	
	/* this is the LOCAL port that the primary will use to send commit requests to the backup
	 backup can check incoming connections against this port to differentiate them from illegal connections */
	public static int commitPort; 
	public static Socket commitSocket = new Socket();
	
	// timeout value to use for blocking read operations (if the client is taking too long to send required data, server is able to timeout
	private static final int TIMEOUT = 5000; // UNIT: ms
	
	private static final Random generator = new Random(); 
	
	
	public static void setPeerStatus (InetSocketAddress server, boolean status) {
		synchronized (mutexLock) {
			peerStatus.put(server, status);
		}
	}
	
	public static boolean isPrimary () {
		return isPrimary;
	}
	
	public static InetSocketAddress getPeerAddress () {
		return peerServer;
	}
	
	public static void insertCommitLSN (LogRecord record) { 
		commitDB.put(record.getCommitLSN(), record);
	}
	
	public static LogRecord getRecordByCommitLSN (int LSN) {
		return commitDB.get(LSN);
	}
	
	
	public static void main (String[] args) {
				
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				
				System.out.println();
				System.out.println();
				
				System.out.println("[*** Graceful shutdown sequence initiated ***]");
				
				if (logFile != null && logFile.exists()) {
					System.out.println("[*** Internal log file deleted; entire transaction history has been wiped ***]");
					while (logFile.delete() == false);
				}
				if (recoveryLogFile != null && recoveryLogFile.exists()) {
					while (recoveryLogFile.delete() == false);
				}
				if (dir == null) {
					return;
				}
				ArrayList<File> serverJunk = getFileList(dir, tempFilePrefix);
				if (serverJunk == null) {
					return;
				}
				for (int i = 0; i < serverJunk.size(); i++) {
					System.out.println("[*** Deleting temporary file: " + serverJunk.get(i).getAbsolutePath() + " ***]");
					while (serverJunk.get(i).delete() == false);
				}
				System.out.println("[*** All temporary files have been wiped clean ***]");
				return;
			}
		});	
		
		HashMap <String, String> options = null;
		try {
			options = getOptions(args, commandLineOptions);
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			printUsage();
			System.exit(1);
		}
		
		try {
			if (!options.containsKey("dir")) {
				System.err.println("Input error: -dir is a required option");
				printUsage();
				System.exit(1);
			} else {				
				dir = options.get("dir");
				if (!new File(dir).exists() && !new File(dir).isDirectory()) {
					System.err.println("Error: Invalid path provided: " + dir);
					System.exit(1);
				}
			}
			
			if (!options.containsKey("primary")) {
				System.err.println("Input error: -primary is a required option");
				printUsage();
				System.exit(1);
			} else {				
				primaryFile = options.get("primary");
				if (!new File(primaryFile).exists() && !new File(dir).isFile()) {
					System.err.println("Error: unable to locate file: " + primaryFile);
					System.exit(1);
				}
			}
			// check and set (if not provided by the user, initialize to default values 
			if (options.containsKey("port")) {
				port = Integer.parseInt(options.get("port"));
			} else {
				port = 8080;
			}
						
			if (options.containsKey("ip")) {
				bindAddr = InetAddress.getByName(options.get("ip"));
			} else {
				bindAddr = InetAddress.getByName("127.0.0.1");
			}
			
		} catch (UnknownHostException e) {
			System.err.println("Input error: unknown host, " + e.getMessage());
			System.exit(1);
		} catch (NumberFormatException e) {
			System.err.println("Input error: port number must be a valid number");
			System.exit(1);
		}
		
		
		// read the primary file and determine the location of the primary server 
		// if the ip address and port are the same as yours, then you are the primary
		try {
			FileInputStream fis = new FileInputStream(primaryFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			String[] address = line.split(" ");
			if (address.length < 2) {
				System.err.println("Invalid syntax of primary file, expected format: <IP_ADDRESS> <PORT> <COMMIT_PORT> \n where <COMMIT_PORT> is updated by the server as necessary, do not provide this value!");
				System.exit(1);
			}
			InetAddress ipaddr = InetAddress.getByName(address[0]);

			int portNumber = Integer.parseInt(address[1]);
			
			if (ipaddr.equals(bindAddr) && portNumber == port) {
				isPrimary = true;
				System.out.println("[** Server will act as primary - not running yet **]".toUpperCase());
			} else {
				System.out.println("[** Server will act as backup - not running yet **]".toUpperCase());
				peerServer = new InetSocketAddress(ipaddr, portNumber);
				peerStatus.put(peerServer, false);
				//commitPort = Integer.parseInt(address[2]);
			}
						
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage() + ", please specify a correct path to the primary file.");
			System.exit(1);
		} catch (UnknownHostException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.err.println(e.getMessage() + ", there was a problem reading the primary file.");
			System.exit(1);
		} catch (NumberFormatException e) {
			System.err.println(e.getMessage() + ", invalid port provided, must be a parsable integer.");
			System.exit(1);
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		if (isPrimary && (!options.containsKey("bip") || !options.containsKey("bport"))) {
			System.err.println("The IP address and port number of the backup server must be provided");
			System.exit(1);
		}
		
		if (!isPrimary && (options.containsKey("bip") || options.containsKey("bport"))) {
			System.err.println("Since this server is currently not acting as the primary, it cannot have a backup: illegal options provided (bip, bport)");
			System.exit(1);
		} 
		
		if (isPrimary) {
		
			try {
				
//				commitPort = findFreePort(); // port primary will use to send COMMIT requests to the backup (and receive the corresponding ACK's)
//				updatePrimary(bindAddr, port, primaryFile);
//				try {
//					commitSocket.bind(new InetSocketAddress(bindAddr, commitPort));
//					updatePrimary(bindAddr, port, primaryFile);
//				}
//				catch (IOException e) {
//					System.err.println("Was unable to bind the commit socket to an available port. Try restarting the program.");
//					System.exit(1);
//				}
				
				peerServer = new InetSocketAddress(InetAddress.getByName(options.get("bip")), Integer.parseInt(options.get("bport")));
			} catch (NumberFormatException e) {
				System.err.println(e.getMessage() + ", invalid port provided, must be a parsable integer.");
				System.exit(1);
			} catch (UnknownHostException e) {
				System.err.println(e.getMessage() + ", backup address is invalid");
				System.exit(1);
			}
		}
		
        final ExecutorService executor = Executors.newCachedThreadPool();
        
        new Thread() {
        	public void run() {
        		while (!isPrimary) {
        			try {
        				/* check if primary/backup is alive, if not, take appropriate action:
        				 * 	if you are the backup, and primary is not responding, make yourself the primary (update the primary file)
        				 *  if you are the primary, and the backup has failed, do nothing (keep listening in case backup comes back online)
        				*/
        				Thread.sleep(2*1000);
        				for (final InetSocketAddress server : peerStatus.keySet()) {
        					Runnable tester = new Pinger(server, 50);
        					executor.execute(tester);
        				}
        				
        			} catch (InterruptedException e) {
        				System.err.println(e.getMessage());
        			}
        		}
        	}
        }.start();


		startRecovery ();
		Socket socket = null;
		try {
			logFile.createNewFile();
			File directory = new File (dir);
			
			// populate and initialize the file access manager - this keeps track of all files located within the 'dir' directory 
			for (final File fileEntry :  directory.listFiles()) {
				if (!fileEntry.isDirectory()) {
					fileAccessManager.put(fileEntry.getName(), 0);
				}
			}
			
//			System.out.println("Number of files found in directory: " + fileAccessManager.size() + " :");
//			for (String fname : fileAccessManager.keySet()) {
//				System.out.println(fname);
//			}
			
			serverSocket = new ServerSocket(port, 0, bindAddr);
			serverSocket.setReuseAddress(true);
			
			System.out.println("[** Server is live and listening on ".toUpperCase() + bindAddr.getHostAddress() + ":" + port + " **]");
			while (true) {	
				socket = serverSocket.accept();
				socket.setTcpNoDelay(true);
				socket.setReuseAddress(true);
				
//				System.out.println(String.format("Incoming from %s:%s", socket.getInetAddress(), socket.getPort()));
				if (isPrimary) {
					new Transaction(socket).start();
				} else {
					updateBackupCommitPort(primaryFile);
					// if you are the backup, ignore all connections except those from the primary
					if (socket.getInetAddress().equals(peerServer.getAddress()) && socket.getPort() == commitPort) {
						new Transaction(socket).start();
					} else { // send an error to all connections that are not from the primary server
						PrintWriter out = null;
						out = new PrintWriter(socket.getOutputStream(),true);
						String message = "Request ignored, forward your request to the current primary server (" + peerServer.getAddress().getHostAddress() + ":" + peerServer.getPort() + ")";
						String response = ClientServerProtocol.buildResponse(ClientServerProtocol.ResponseMethod.ERROR,
								-1, 
								-1, 
								ClientServerProtocol.Error.INVALID_OPERATION, 
								message.length(),
								message);
						out.print(response);
						out.flush();
					}
				}
			}
		} catch (IOException e) {
			System.err.println("IOException: " + e.getMessage());
			System.exit(1);	
		} catch (IllegalArgumentException e) {
			System.err.println("Port parameter is outside the specified range of valid port values, should between 0 and 65535, inclusive.");
			System.exit(1);
		} finally {
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
				}
			}
		}
	}
	
	
	public static int findFreePort() {
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(0);
			socket.setReuseAddress(true);
			int port = socket.getLocalPort();
			try {
				socket.close();
			} catch (IOException e) {
				// Ignore IOException on close()
			}
			return port;
		} catch (IOException e) { 
		} finally {
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
				}
			}
		}
		throw new IllegalStateException("Could not find a free TCP/IP port, restart the program.");
	}
	
	
	public static void updateBackupCommitPort (String primaryFile) {
		if (!isPrimary) {
			try {
				FileInputStream fis = new FileInputStream(primaryFile);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				String line = br.readLine();
				String[] address = line.split(" ");
				if (address.length < 2) {
					System.err.println("Invalid syntax of primary file, expected format: <IP_ADDRESS> <PORT> <COMMIT_PORT> \n where <COMMIT_PORT> is updated by the server as necessary, you do not need to provide this.");
					System.exit(1);
				}
				commitPort = Integer.parseInt(address[2]);

			} catch (FileNotFoundException e) {
				System.err.println(e.getMessage() + ", please specify a correct path to the primary file.");
				System.exit(1);
			} catch (IOException e) {
				System.err.println(e.getMessage() + ", there was a problem reading the primary file.");
				System.exit(1);
			} catch (NumberFormatException e) {
				System.err.println(e.getMessage() + ", invalid port provided, must be a parsable integer.");
				System.exit(1);
			} catch (IllegalArgumentException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
		}
	}
	
	
	/* UPDATES THE PRIMARY SERVER ADDRESS 
	 * 
	 * Syntax of primary file (first line contains the address:port of the primary): 
	 		<IP_ADDRESS> <PORT>  <COMMIT_PORT>
	*/
	public synchronized static void updatePrimary (InetAddress address, int port, String primaryFile) {
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(primaryFile);
			StringBuilder sb = new StringBuilder();
			sb.append(address.getHostAddress());
			sb.append(" ");
			sb.append(port);
			sb.append(" ");
			sb.append(commitPort);
			writer.print(sb.toString());
			//System.out.println("Server is now running as the primary");
		} catch (FileNotFoundException e) {
			System.err.println("Could not locate primary file, exiting program");
			System.exit(1);
			e.printStackTrace();
		} finally {
			isPrimary = true;
			if (writer != null) {
				writer.close();
			}
		}
	}

	
	
	public static void removeCommit (int transactionID) {
		mutex.lock();
		try {
			while (commitInProgress) {
				try {
					removeCommitCond.await();
				} catch (InterruptedException e) {
					// silent exception
				}
			}
			synchronized (mutexLock) {
				//System.out.println("Remove commit: " + transactionID);
				commitOrder.remove(transactionID);
			}
		} finally {
			mutex.unlock();
		}	
//		while (true) {
//			if (!commitInProgress) {
//				synchronized (mutexLock) {
//					commitOrder.remove(transactionID);
//					break;
//				}
//			}
//			int sleepTime = generator.nextInt((15-8) + 1) + 8;
//			try {
//				Thread.currentThread().sleep(2*sleepTime);
//			} catch (InterruptedException e) {
//			}
//		}
	}
	
	public static void addCommit (int transactionID) {
		commitQueue.lock();
		try {
			while (commitInProgress) {
				try {
					addCommitCond.await();
				} catch (InterruptedException e) {
					// silent exception
				}
			}
			
//			System.out.println("addCommit: " + transactionID);
			commitOrder.put(transactionID, null);
		} finally {
			commitQueue.unlock();
		}
		
		
		
		
//		System.out.println("Adding commit: " + transactionID);
//		synchronized (commitQueue) {
//			while (true) {
//				System.out.println("Waiting: " + transactionID);
//				if (!commitInProgress) {
//					System.out.println("Got here: " + transactionID);
//					commitOrder.put(transactionID, null);
//					break;
//				}
//			}
//		}
	}
	
	public static synchronized void processCommits() {
		commitInProgress = true;
		LogRecord newRecord = null;
		LogRecord transaction = null;
		ArrayList<Integer> toRemove = new ArrayList<Integer>();
		try {
			for (Integer i : commitOrder.keySet()) {
				try {
					
					transaction = getLog (i.intValue());
					//transaction = transactionDB.get(i);
					
					if (transaction.hasCommitted()) {
						newRecord = new LogRecord(
								RequestMessage.RequestMethod.COMMIT,
								transaction.getTransactionID(), 
								transaction.getSequenceNumber(), 
								null,
								null);
						addLog(newRecord);
						//System.out.println("Processing commit, TID:" + transaction.getTransactionID());
						toRemove.add(i);
					}

				} catch (ServerException e) { }
			}

			for (Integer i : toRemove) {
				synchronized (mutexLock) {
					commitOrder.remove(i);
				}
			}
			
		} finally {
			mutex.lock();
			commitQueue.lock();
			commitInProgress = false;
			addCommitCond.signal();
			removeCommitCond.signal();
			commitQueue.unlock();
			mutex.unlock();
		}

		//		Iterator<Integer> iter = commitOrder.keySet().iterator();
		//
		//		// for (Integer entry : commitOrder.keySet()) {
		//		while (iter.hasNext()) {
		//			try {
		//				Integer entry = iter.next();
		//				log = getLog(entry.intValue());
		//				newRecord = new LogRecord(RequestMessage.RequestMethod.COMMIT,
		//						log.getTransactionID(), log.getSequenceNumber(), null,
		//						null);
		//
		//				if (log.hasCommitted()) {
		//					// System.out.println(newRecord.getTransactionID());
		//					addLog(newRecord);
		//					iter.remove();
		//				}
		//			} catch (ServerException e) {
		//
		//			} finally {
		//				commitInProgress = false;
		//			}
		//		}
	}

	private static void printUsage () {
		System.out.println("USAGE: ");
		System.out.println("java FileServer -ip [ip_address_string] -port [port_number] -dir <directory_path> -primary <primary_file_path> -bip <backup_ip_address> -bport <backup_port_number>");
		System.out.println("The following options are available: \n"
				+ "-dir \t\t (required) Absolute path to the server's filesystem (i.e. /Users/john/Desktop/tmp/) \n"
				+ "-primary \t (required) Absolute path to the file that contains the address:port of the primary server \n"
				+ "-ip \t\t IP address to bind to (default: 127.0.0.1) \n"
				+ "-port \t\t Port number to bind to (default: 8080) \n"
				+ "-bip \t\t IP address of the backup server (only provide this field if you are starting the primary server \n"
				+ "-bport \t\t Port number of the backup server (only provide this field if you are starting the primary server \n");
		System.out.println();
		System.out.println("Syntax of primary file: \n".toUpperCase()
				+ "[IP_ADDRESS] [PORT] [COMMIT_PORT] \n[COMMIT_PORT] is updated by the server as necessary, you do not need to provide this value.");

	}
	
	public static int getTimeout () {
		return TIMEOUT;
	}
	
	/* parse log file and recover system to a stable state right before the crash */
	private static void startRecovery () {
		RequestMessage.RequestMethod method;
		int transactionID;
		int sequenceNumber;
		String data;
		boolean flushed;
		
		LogRecord transactionLog;
		LogRecord newRecord;
		Map<Integer, LogRecord> flushJobs = new LinkedHashMap<Integer, LogRecord>();
		logFile = new File(dir,logName);

		
		// delete any temporary files created by the server that may be present in the directory
		ArrayList<File> serverJunk = getFileList(dir, tempFilePrefix);
		for (int i = 0; i < serverJunk.size(); i++) {
			while (serverJunk.get(i).delete() == false);
		}
		
		try {
			logFile.createNewFile();
		} catch (IOException e1) {
		}
		
			
		BufferedReader reader = null;

		if (logFile.exists()) {
			recoveryLogFile = new File (dir, tempFilePrefix+recoveryLogName);
			try {
				recoveryLogFile.createNewFile();
			} catch (IOException e1) {
			}
			
			// create a copy of the log and walk through the copy making any changes as necessary and flushing any unflushed commits 
			
			try {

				reader = new BufferedReader(new FileReader(logFile));
				String line;
				String [] logHeader;
				
				while ((line = reader.readLine()) != null) {
					line = fromHexString(line); // decode from hex to String
					logHeader = line.split(LogRecord.getDecodeDelimiter());
					
//					for (int i = 0; i < logHeader.length; i++) {
//						System.out.print(logHeader[i] + " ");
//					}
//					System.out.println();

					method = RequestMessage.RequestMethod.fromString(logHeader[0]);
					transactionID = Integer.parseInt(logHeader[1]);
					sequenceNumber = Integer.parseInt(logHeader[2]);
					
					switch (method) {
					case ABORT: 	 /* method<>tid<>seq */
					{
						transactionLog = transactionDB.get(transactionID);
						newRecord = new LogRecord (
								method, 
								transactionID, 
								-1, 
								null, 
								null);
						transactionLog.setAborted(true);
						transactionLog.addLog(newRecord);
					}
					break;
					case COMMIT: 	 /* method<>tid<>seq<>flushed */
					{
						transactionLog = transactionDB.get(transactionID);
						newRecord = new LogRecord (
								method, 
								transactionID, 
								sequenceNumber, 
								null, 
								null);
						transactionLog.setCommited(true);
						transactionLog.setReceivedCommitRequest(true);
						transactionLog.setSequenceNumber(sequenceNumber);
						transactionLog.applyCommitLSN(Integer.parseInt(logHeader[3]));
						flushed = Boolean.parseBoolean(logHeader[4]);
						
//						System.out.println("Recovery parsed a COMMIT log: ");
//						System.out.println(method.toString() + " " + transactionID + " " + sequenceNumber + " " + flushed);
//						System.out.println();

						flushJobs.put(transactionID, transactionLog);
						
						if (flushed) {
//							System.out.println("Inside flushed, TID: " + transactionID);
							newRecord.setFlushed(true);
							FileServer.addLog(newRecord);
							flushJobs.remove(transactionID);
						} else {
							LogRecord.setLSN(Integer.parseInt(logHeader[3]));
							commitDB.put(Integer.parseInt(logHeader[3]), transactionLog);
						}
					}
						break;
					case NEW_TXN:	 /* method<>tid<>seq<>data */ 
					{
						data = logHeader[3]; // data represents the filename in this case
						transactionLog = new LogRecord(null, transactionID, -1, null, null);
						newRecord = new LogRecord (
								method, 
								transactionID, 
								0, 
								data, 
								data);
						transactionDB.put(transactionID, transactionLog);
						transactionLog.setFilename(data);
						transactionLog.addLog(newRecord);
					}
						break;	
					case WRITE:		 /* method<>tid<>seq<>data */ 		
					{
						data = logHeader[3]; 
						transactionLog = transactionDB.get(transactionID);
						newRecord = new LogRecord ( 
								method, 
								transactionID, 
								sequenceNumber,  
								null, 
								data);
						
						transactionLog.addLog(newRecord);
					}
						break;
					default:
						break;
					}
				}
				
				
				for (Integer tid : flushJobs.keySet()) {
					transactionLog = flushJobs.get(tid);
					newRecord = new LogRecord (
							RequestMessage.RequestMethod.COMMIT, 
							transactionLog.getTransactionID(), 
							transactionLog.getSequenceNumber(), 
							null, 
							null);
					System.out.println("Attempting to fully commit to disk TID: " + tid);

					FileServer.addLog(newRecord);
				}
				
				
				// IF YOU ARE THE BACKUP, SYNC with primary 

				Socket primary = null;
				if (!isPrimary) {
					try {
						ServerMessage syncRequest = new ServerMessage(ServerMessage.RequestMethod.SYNC, LogRecord.getCurrentLSN());
						InetSocketAddress primaryAddr = FileServer.getPeerAddress();
						//Socket primary = new Socket(primaryAddr.getAddress(), primaryAddr.getPort());
						try {
							primary = new Socket();
							primary.bind(new InetSocketAddress(bindAddr, port));
						} catch (IOException e) {
							System.err.println("Port: " + port + " is currently taken by your Operating System, try running the program again after a few seconds. \n It usually takes some time (~ close to a minute) for the operating system to release the port. Exiting program...");
							System.err.println("This was likely due to killing the server and attempting to quickly restart it. The operating system does not immediatley release ports if a program was interrupted abnormally.");

							System.exit(1);
						}
						
						primary.connect(primaryAddr);
						primary.setTcpNoDelay(true);
						primary.setReuseAddress(true);

						ObjectOutputStream os = new ObjectOutputStream(primary.getOutputStream());
						os.writeObject(syncRequest);

						os.flush();

						ObjectInputStream inputStream = new ObjectInputStream(primary.getInputStream());

						ArrayList<LogRecord> missingTransactions = (ArrayList<LogRecord>) inputStream.readObject();
						if (missingTransactions.size() > 0) {
							System.out.println(String.format("[** Backup is synchronizing %s transactions with the primary - do not kill either server **]".toUpperCase(), missingTransactions.size()));
						}

						LogRecord toCommit = null;
						for (int i = 0; i < missingTransactions.size(); i++) {
							LogRecord transaction = missingTransactions.get(i);
							transaction.setCommited(true);
							System.out.println("Syncing TID: " + transaction.getTransactionID());
							transactionDB.put(transaction.getTransactionID(), transaction);
							commitDB.put(transaction.getCommitLSN(), transaction);
							toCommit = new LogRecord (
									RequestMessage.RequestMethod.COMMIT, 
									transaction.getTransactionID(), 
									transaction.getSequenceNumber(), 
									null, 
									null);
							LogRecord.setLSN(transaction.getCommitLSN());
							manageFile(transaction.getFileName());
							addLog(toCommit);
						}
						
						if (missingTransactions.size() > 0) {
							System.out.println("[** Backup is now synchronized with the primary **]".toUpperCase());
						}


					} catch (ClassNotFoundException e) {
					} catch (IOException e) {
						// primary is not responding! -- become the new primary (update the primary.txt file)						
						System.out.println("The primary server was not reachable while the backup server was undergoing recovery (replication could not be completed), "
								+ "backup will now act as the new primary. Make sure you to start your primary server first to avoid this error.");
						System.out.println(String.format("[** Promoted to primary - %s:%s **]".toUpperCase(), FileServer.bindAddr.getHostAddress(), FileServer.port));
						FileServer.updatePrimary(bindAddr, port, primaryFile);
						
						//e.printStackTrace();
					} finally {
						if (primary != null) 
							primary.close();
					}
				}
				
				reader.close();
				
				if (logFile.exists()) {
					while(logFile.delete() == false);
				}
				
				boolean nameChanged = false;
				
				do {
					nameChanged = recoveryLogFile.renameTo(logFile);
				} while (!nameChanged);
								
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ServerException e) {
				e.printStackTrace();
			} finally {
				try {
					if (reader != null)
						reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}		
		} 

		inRecoveryMode = false;
		return;
	}
	
	/* returns a list files from path with names that start with the pattern argument */
	public static ArrayList<File> getFileList (String path, final String prefix) {
		ArrayList<File> fileList = new ArrayList<File>();
		if (path == null || prefix == null ) return null;
		File directory = new File(path);
		File[] files = directory.listFiles( new FilenameFilter() {
			
			@Override
			public boolean accept(File directory, String name) {
				return name.startsWith(prefix);
			}
		});
		
		if (files == null) {
			return null;
		}
				
		for (File file : files) {
			if (file.exists() && file.isFile()) {
				fileList.add(file);
			}
		}
		
		return fileList;
	}

	public synchronized static LogRecord getLog (int tid) throws ServerException {
		if (isTaken(tid)) {
			return transactionDB.get(tid);
		} else {
			throw new ServerException("TID (" + tid + ") does not refer to a valid transaction.", ClientServerProtocol.Error.INVALID_TRANSACTION_ID);
		}
	}

	// initializes transactions by generating a unique TID and setting up the log data structure 
	public synchronized static int generateID () {
		int newID = generator.nextInt((99999-10000) + 10000);
		if (isTaken(newID)) generateID();
		LogRecord transactionLog = new LogRecord(null, newID, -1, null, null);
		transactionDB.put(newID, transactionLog);
		return newID;
	}
	
	public synchronized static void manageFile (String fname) {
		if (!fileAccessManager.containsKey(fname)) {
			fileAccessManager.put(fname, 0);
		}
	}

	
	// checks if a transaction ID has been used (i.e. valid) 
	 
	public synchronized static boolean isTaken (int tid) {
		if (transactionDB.containsKey(tid)) {
			return true;
		} else {
			return false;
		}
	}
	
	public static String toHexString (String toEncode) {
		return DatatypeConverter.printHexBinary(toEncode.getBytes());
	}
	
	public static String fromHexString (String toDecode) {
		return new String(DatatypeConverter.parseHexBinary(toDecode));
	}
	
	public static void copyFile(File source, File destination) {
	    InputStream in = null;
	    FileOutputStream out = null;
		BufferedWriter bw = null;

		try {
			in = new FileInputStream(source);
			out = new FileOutputStream(destination);
		    // Transfer bytes from in to out
		    byte[] buf = new byte[(int) source.length()];
		    int len;
		    while ((len = in.read(buf)) > 0) {
		        out.write(buf, 0, len);
		    }
		    
		    out.flush();
			out.getChannel().force(true);
			out.getFD().sync();
			out.getFD().sync();
		    
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		} finally {
			 try {
				if (in != null)
					in.close();
				if (out != null)
					out.close();
			} catch (IOException e) {

			}
		} 
	}
	
	/* WRITE AHEAD LOGGING :
	 * Gets called to append a new log entry EVERY time a valid request of type [NEW_TXN, WRITE, ABORT] is received 
	 * 
	 * Note: this method needs to be THREAD-SAFE
	 */
	
	public static void addLog (LogRecord newEntry) {
		
		File log = logFile;
		if (inRecoveryMode) {
			log = recoveryLogFile;
		}

		FileOutputStream outputStream = null;
		BufferedWriter bw = null;
		
		try {
			LogRecord transactionLog = getLog(newEntry.getTransactionID());
			File file = new File (dir, transactionLog.getFileName());
			File tempFile = new File (dir , tempFilePrefix+transactionLog.getFileName());
			
			if (newEntry.getMethod() == RequestMessage.RequestMethod.COMMIT && newEntry.hasFlushed()) {
				
				if (tempFile.exists()) {
					
//					System.out.println("Simulating crash during commiting, there should be a temp file now");
//					try {
//						Thread.currentThread().sleep(100000);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
										
					if (file.exists()) {
						while (file.delete() == false);
					}

					boolean nameChanged = false;

					do {
						nameChanged = tempFile.renameTo(file);
					} while (!nameChanged);
				}
				
				while (true) {
					synchronized (commitLock) {
						if (fileAccessManager.containsKey(file.getName())) {
							fileAccessManager.put(file.getName(), 0);
							break;
						}
					}
					
					try {
						int idle = generator.nextInt((3-1) + 1) + 1;
						Thread.sleep(idle*5);
					} catch (InterruptedException e) {
						
					}
				}
				
			}
			
			synchronized (logLock) {

				outputStream = new FileOutputStream(log, true);
				bw = new BufferedWriter(new OutputStreamWriter(outputStream));
				///			System.out.println("New log entry: " + newEntry.toString());  // ***************************************
				bw.write(toHexString(newEntry.toString()));
				bw.newLine();
				bw.flush();

				// force flushes the log entry to disk (not 100% guaranteed, but very very likely) WRITE AHEAD LOGGING
				outputStream.getChannel().force(true);
				outputStream.getFD().sync();
				outputStream.getFD().sync();
				
				

			}
			
			/* if the method is COMMIT we need to update the file the transaction is referring to and flush all updates to disk
			 *  - Bundle all of the data for the transaction in question
			 *	- Create a temporary hidden file that mirrors the file that the transaction will be appending to (or a new file if the filename does not exist yet)
			 *	- After all updates have been done to the temporary file, delete original file and rename the temporary file to the original and force flush 
			 *	- Finally add a new log entry that records that the COMMIT has been flushed
			 */
			if (newEntry.getMethod() == RequestMessage.RequestMethod.COMMIT && !newEntry.hasFlushed()) {
//				LogRecord transactionLog = getLog(newEntry.getTransactionID());
//				File file = new File (dir, transactionLog.getFileName());
//				File tempFile = new File (dir , tempFilePrefix+transactionLog.getFileName());
				String dataToAppend = transactionLog.getCommittedData();				
				while (true) {
					synchronized (commitLock) {
						if (fileAccessManager.containsKey(file.getName())) {
							if (fileAccessManager.get(file.getName()) == 0) {
								fileAccessManager.put(file.getName(), -1);
								break;
							}
						} else {
							break;
						}
					}
					//System.out.println("waiting");
				}
				
				//System.out.println(transactionLog.getTransactionID() + " is committing to disk");
				
				if (file.exists()) {
					// create a copy of the file we intend to update
					copyFile(file, tempFile);	
				} 

				// append data to the temp file
				outputStream = new FileOutputStream(tempFile, true);
				//System.out.println("Data to append: " + dataToAppend);
				bw = new BufferedWriter(new OutputStreamWriter(outputStream));
				bw.write(dataToAppend);
				bw.flush();

				outputStream.getChannel().force(true);
				outputStream.getFD().sync();
				outputStream.getFD().sync();
				
//				if (file.exists()) {
//					file.delete();
//				}
//				
//				boolean nameChanged = false;
//				
//				do {
//					nameChanged = tempFile.renameTo(file);
//				} while (!nameChanged);
				
				newEntry.setFlushed(true); 
			
				addLog (newEntry);
			}
			
		} catch (FileNotFoundException e) {

		} catch (IOException e) {

		} catch (ServerException e) {
			System.out.println("addLog: ServerException");
			System.out.println(e.getMessage());

		} finally {
			try {
				if (outputStream != null) {
					outputStream.close();
				}
			} catch (IOException e) {
				// muted exception, no need to escalate 
		    }
		}
	}
	
	
	public static String readFile (String filename) throws ServerException {
		
		while (true) {
			synchronized (commitLock) {
				if (fileAccessManager.containsKey(filename)) {
					if (fileAccessManager.get(filename) >= 0) {
						fileAccessManager.put(filename, fileAccessManager.get(filename) + 1);
						break;
					}
				} else {
					break;
				}
			}
		}
		
		File file = new File (dir, filename);

	    if ( file.length() > Integer.MAX_VALUE ) {
	        throw new ServerException(String.format("File (%s) is too large", file.getName()), ClientServerProtocol.Error.INVALID_OPERATION);
	    }
	    

	    ByteArrayOutputStream os = null;
	    InputStream ios = null;
	    try {
	        byte[] buffer = new byte[(int) file.length()];
	        os = new ByteArrayOutputStream();
	        ios = new FileInputStream(file);
	        int read = 0;
	        while ( (read = ios.read(buffer)) != -1 ) {
	            os.write(buffer, 0, read);
	        }
	    } catch (FileNotFoundException e) {
	    	throw new ServerException(e.getMessage(), ClientServerProtocol.Error.FILE_NOT_FOUND);
	    } catch (IOException e) {
	    	throw new ServerException(e.getMessage(), ClientServerProtocol.Error.FILE_IO_ERROR);
	    }
	    finally { 
	    	
			while (true) {
				synchronized (commitLock) {
					if (fileAccessManager.containsKey(filename)) {
						if (fileAccessManager.get(filename) > 0) {
							fileAccessManager.put(filename, fileAccessManager.get(filename) - 1);
							break;
						}
					} else {
						break;
					}
				}
			}
	    	
	        try {
	             if ( os != null ) 
	                 os.close();
	        } catch ( IOException e) {
	        }

	        try {
	             if ( ios != null ) 
	                  ios.close();
	        } catch ( IOException e) {
	        }
	    }
	    //System.out.println(filename + " is " + os.toString().length() + " bytes, actual size is  " + file.length());
	    return os.toString();
	}

	private static HashMap<String, String> getOptions (String[] args, String[] programOptions) {
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
