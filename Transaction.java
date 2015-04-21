import java.net.*;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;

public class Transaction extends Thread {

	private Socket clientSocket = null;
	private PushbackInputStream pbis = null;
	private DataInputStream in = null;
	private PrintWriter out = null;
	private ByteArrayOutputStream os = null;
	private static final int IGNORE = -1; 
	private static final Object mutex = new Object();
	private static Lock commitQueue = new ReentrantLock(true);


	public Transaction (Socket clientSocket) {
		//System.out.println("New client connection");
		this.clientSocket = clientSocket;
		
		try {
			this.clientSocket.setTcpNoDelay(true);
			this.clientSocket.setReuseAddress(true);
			//this.clientSocket.setSoTimeout(FileServer.getTimeout());
			pbis = new PushbackInputStream(this.clientSocket.getInputStream(), 1);
			in = new DataInputStream(pbis);
			out = new PrintWriter(this.clientSocket.getOutputStream(),true);
		} catch (SocketException e) {
			System.err.println ("SocketException: " + e.getMessage());
		} catch (IOException e) {
			System.err.println ("IOException: " + e.getMessage());
		} 
	}

	
	/* Called to send COMMIT requests to the backup for replication */
	public static void backupSYNC (LogRecord transactionRecord) {
		commitQueue.lock();
		try {
			if (transactionRecord.hasReceivedBackupACK()) return;
			int timeout = 600;
			FileServer.commitPort = FileServer.findFreePort(); // port primary will use to send COMMIT requests to the backup (and receive the corresponding ACK's)
			FileServer.updatePrimary(FileServer.bindAddr, FileServer.port, FileServer.primaryFile);

			InetSocketAddress peerAddress = FileServer.getPeerAddress();
			System.out.println(String.format("Try to send COMMIT for (TID:%d, CLSN:%d) to backup", transactionRecord.getTransactionID(), transactionRecord.getCommitLSN()));
			Socket socket = null;
			ObjectOutputStream os;
			ObjectInputStream inputStream;
			ServerMessage response;

			try {
				socket = new Socket();
				socket.bind(new InetSocketAddress(FileServer.bindAddr, FileServer.commitPort));
				//socket = new Socket(peerAddress.getAddress(), peerAddress.getPort());
				//FileServer.commitSocket = new Socket();
				socket.connect(new InetSocketAddress(peerAddress.getAddress(), peerAddress.getPort()));
				socket.setTcpNoDelay(true);
				socket.setReuseAddress(true);
				socket.setSoTimeout(timeout);

				os = new ObjectOutputStream(socket.getOutputStream());
				os.writeObject(transactionRecord);
				os.flush();

				inputStream = new ObjectInputStream(socket.getInputStream());

				while (true) {
					response = (ServerMessage) inputStream.readObject();
					switch (response.getResponseMethod()) {
					case ACK:
						if (response.getCommitLSN() == transactionRecord.getCommitLSN()) {
							System.out.println(String.format("Received ACK for (TID:%d, CLSN:%d) from backup", transactionRecord.getTransactionID(), response.getCommitLSN()));
							transactionRecord.setBackupACK(true);
							return;
						}
						break;
					case ASK_RESEND:
						int expectedLSN = response.getCommitLSN();
						LogRecord toSend = FileServer.getRecordByCommitLSN(expectedLSN);
						System.out.println("Received ASK_RESEND request for CLSN: " + expectedLSN);
						os.writeObject(toSend);
						os.flush();
						break;
					default:
						break;

					}
				}

			} catch (SocketTimeoutException e) {
				//System.err.println("No response received from backup after " + timeout + "ms , timing out.");
				return;
			} catch (IOException e) {
//				System.err.println(e.getMessage());
//				System.err.println("Unable to connect to backup, continue...");
				return;
			} catch (ClassNotFoundException e) {
				// this should never happen
			} finally {
				if (socket != null) {
					try {
						socket.close();
					} catch (IOException e) {
					}
				}
			}
		} finally {
			commitQueue.unlock();
		}
	}

	@Override
	public void run () {
		
		// if you are the backup, listen for COMMIT requests from the primary 
		if (!FileServer.isPrimary()) {
			ServerMessage message = null;
			ObjectOutputStream oos = null;
			ObjectInputStream inputStream = null;
			
			try {
				inputStream = new ObjectInputStream(clientSocket.getInputStream());
				oos = new ObjectOutputStream(clientSocket.getOutputStream());
				LogRecord logRecord = (LogRecord) inputStream.readObject();
				System.out.println("Received CLSN: " + logRecord.getCommitLSN());
				// ensures that commit records are received and applied in proper order
				int expectedLSN = LogRecord.getCurrentLSN()+1;
				int receivedLSN = logRecord.getCommitLSN();
				while (expectedLSN != receivedLSN) {
					System.out.println("Sending ASK_RESEND for LSN: " + expectedLSN);
					message = new ServerMessage(ServerMessage.ResponseMethod.ASK_RESEND, expectedLSN);
					oos.writeObject(message);
					oos.flush();
					logRecord = (LogRecord) inputStream.readObject();
					receivedLSN = logRecord.getCommitLSN();
					System.out.println("Waiting for commitLSN: " + expectedLSN + " to arrive, received: " + logRecord.getCommitLSN());
//					try {
//						Thread.currentThread().sleep(10);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
				}
				FileServer.manageFile(logRecord.getFileName());
				LogRecord.setLSN(logRecord.getCommitLSN());
				LogRecord newRecord = new LogRecord (
						RequestMessage.RequestMethod.COMMIT, 
						logRecord.getTransactionID(), 
						logRecord.getSequenceNumber(), 
						null, 
						null);
				FileServer.transactionDB.put(logRecord.getTransactionID(), logRecord);
				FileServer.commitDB.put(logRecord.getCommitLSN(), logRecord);
				FileServer.addCommit(logRecord.getTransactionID());
				FileServer.processCommits();
				//FileServer.addLog(newRecord);
				message = new ServerMessage(ServerMessage.ResponseMethod.ACK, logRecord.getCommitLSN());
				oos.writeObject(message);
				oos.flush();
				logRecord.setBackupACK(true);
				return;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
			} finally {
				try {
					clientSocket.close();
				} catch (IOException e) {
				}
			}
		}
		
		// check if request message is from the backup - assuming you are currently the primary server 
		// && clientSocket.getPort() == FileServer.getPeerAddress().getPort()
		if (FileServer.isPrimary() && (clientSocket.getInetAddress().equals(FileServer.getPeerAddress().getAddress()) && clientSocket.getPort() == FileServer.getPeerAddress().getPort())) {
			try {
				ServerMessage message = null;
				ObjectOutputStream oos = null;
				ObjectInputStream inputStream = null;
				clientSocket.setSoTimeout(1200);
				try {
					inputStream = new ObjectInputStream(clientSocket.getInputStream());
					oos = new ObjectOutputStream(clientSocket.getOutputStream());
					message = (ServerMessage) inputStream.readObject();
				} catch (IOException e) {
					return;
				} catch (ClassNotFoundException e) {
					return;
				}

				switch (message.getRequestMethod()) {
				case SYNC:
					int backupLSN = message.getCommitLSN()+1;
					LogRecord record;
					ArrayList<LogRecord> missingTransactions = new ArrayList<LogRecord>();
					for (int i = backupLSN; i <= LogRecord.getCurrentLSN(); i++) {
						record = FileServer.getRecordByCommitLSN (i);
						missingTransactions.add(record);
					}

					try {
						oos.reset();
						oos.writeObject(missingTransactions);
						oos.flush();
					} catch (IOException e) {
						e.printStackTrace();
						return;
					}

					break;
				default:
					break; 
				}
				return;
			} catch (SocketException e1) {
				// TODO Auto-generated catch block
			} finally {
				if (clientSocket != null) {
					try {
						clientSocket.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
					}
				}
			}
		} 
		

		LogRecord log;
		LogRecord newRecord;
		
		try {
			
			int singleByte;
			while ((singleByte = pbis.read()) != -1) {
				
				//this.clientSocket.setSoTimeout(FileServer.getTimeout());
				
				pbis.unread(singleByte);

				RequestMessage request = ClientServerProtocol.parseMessage(in);
				int transactionID = request.getTransactionID();

				switch (request.getMethod()) {
				case ABORT:
				{
					log = FileServer.getLog(transactionID);

					if (log.hasCommitted()) {
						throw new ServerException(String.format("TID: %d has committed, you cannot ABORT a commited transaction.", transactionID), ClientServerProtocol.Error.INVALID_OPERATION);
					}

					newRecord = new LogRecord (
							request.getMethod(), 
							transactionID, 
							IGNORE, 
							null, 
							null);

					log.setAborted(true);
					log.addLog(newRecord);

					String response = ClientServerProtocol.buildResponse(
							ClientServerProtocol.ResponseMethod.ACK, 
							transactionID,  // return the generated transaction ID to the client
							IGNORE, 
							ClientServerProtocol.Error.NONE, 
							0,
							null);

					out.print(response); 
					out.flush();
				}
				break;
				case COMMIT:
				{
					
					
					// the sequence number for a COMMIT request represents that of the LAST write request for this transaction
					log = FileServer.getLog(transactionID);



					if (log.hasCommitted() && request.getSequenceNumber() != log.getSequenceNumber()) {
						FileServer.removeCommit(log.getTransactionID());
						throw new ServerException(String.format("TID:%d has already commited with a different sequence number (%d)", transactionID, log.getSequenceNumber()), ClientServerProtocol.Error.INVALID_OPERATION);
					}

					if (log.hasAborted()) {
						FileServer.removeCommit(log.getTransactionID());
						throw new ServerException(String.format("TID:%d has aborted, you cannot commit an aborted transaction.", transactionID), ClientServerProtocol.Error.INVALID_OPERATION);
					}
					
					if (!log.hasReceivedCommitRequest()) {
						// officially sets the sequence number the server will use to commit this transaction (anything else will be an error)
						log.setSequenceNumber(request.getSequenceNumber()); 
						log.setReceivedCommitRequest(true);
					}

					if (!log.hasCommitted() && log.hasReceivedCommitRequest() && (request.getSequenceNumber() != log.getSequenceNumber())) {
						//FileServer.removeCommit(log.getTransactionID());
						throw new ServerException(String.format("TID:%d has already been requested to commit with a different sequence number (%d)", transactionID, log.getSequenceNumber()), ClientServerProtocol.Error.INVALID_OPERATION);
					}

					ArrayList<Integer> missingSequenceNumbers = log.getMissingSequenceNumbers(log.getSequenceNumber());

					if (!missingSequenceNumbers.isEmpty()) {

						for (Integer missingNumber : missingSequenceNumbers) {
							String response = ClientServerProtocol.buildResponse(
									ClientServerProtocol.ResponseMethod.ASK_RESEND, 
									transactionID,  // return the generated transaction ID to the client
									missingNumber, 
									ClientServerProtocol.Error.NONE, 
									0,
									null);

							//FileServer.removeCommit(log.getTransactionID());
							out.print(response); 
							out.flush();
						}

					} else {

						if (request.getSequenceNumber() != log.getSequenceNumber()) {
							FileServer.removeCommit(log.getTransactionID());
							throw new ServerException(String.format("The server is expecting sequence number %d in order to commit as this number had been requested in an earlier request", log.getLargestSequenceNumber()), ClientServerProtocol.Error.INVALID_OPERATION);
						}


						if (!log.hasCommitted()) {
							newRecord = new LogRecord (
									request.getMethod(), 
									transactionID, 
									request.getSequenceNumber(), 
									null, 
									null);

							log.setCommited(true);
							log.setSequenceNumber(request.getSequenceNumber());
							log.setCommitLSN();
							//log.setSequenceNumber(request.getSequenceNumber());
							//log.addLog(newRecord);
							
							//FileServer.addLog (newRecord); // since the sequence number has already been used, we just add straight to log and flush to disk 
							FileServer.processCommits();
						} else {
							FileServer.removeCommit(transactionID);
						}


						String response = ClientServerProtocol.buildResponse(
								ClientServerProtocol.ResponseMethod.ACK, 
								transactionID,  // return the generated transaction ID to the client
								IGNORE, 
								ClientServerProtocol.Error.NONE, 
								0,
								null);
						
						// if you are the primary server, do not send ACK to client until you have received ACK from backup
						// perhaps put this in a loop until backup has responded 
						if (FileServer.isPrimary() && !log.hasReceivedBackupACK()) {
							backupSYNC(log);
						}
//							out.print(response); 
//							out.flush();
//						} else {
//							out.print(response); 
//							out.flush();
//						}	
						
						out.print(response); 
						out.flush();
					}	
				}
				break;
				case NEW_TXN: /* create a new log record for the new transaction, generate a new ID and add it to the active transactions table */
				{
					
					transactionID = FileServer.generateID();
					String filename = request.getData();
					newRecord = new LogRecord (
							request.getMethod(), 
							transactionID, 
							0, 
							filename, 
							filename);


					log = FileServer.getLog(transactionID);
					log.setFilename(filename);
					FileServer.manageFile(filename);
					log.addLog(newRecord);

					String response = ClientServerProtocol.buildResponse(
							ClientServerProtocol.ResponseMethod.ACK, 
							transactionID,  // return the generated transaction ID to the client
							request.getSequenceNumber(), 
							ClientServerProtocol.Error.NONE, 
							0,
							null);
					
					out.print(response); 
					out.flush();

				}

				break;
				case READ:
				{
					String data = FileServer.readFile (request.getData());
					int contentLength = data.getBytes().length;
					String response = ClientServerProtocol.buildResponse(
							ClientServerProtocol.ResponseMethod.ACK, 
							IGNORE, 
							IGNORE, 
							ClientServerProtocol.Error.NONE, 
							contentLength,
							data);

					out.print(response); 
					out.flush();

				}

				break;
				case WRITE:
				{
					log = FileServer.getLog(transactionID); // does necessary checks for a valid TID

					if (log.hasCommitted() || log.hasAborted()) {
						throw new ServerException(String.format("TID: %d has already committed/aborted.", transactionID), ClientServerProtocol.Error.INVALID_OPERATION);
					}

					newRecord = new LogRecord ( // does necessary check for duplicate sequence numbers
							request.getMethod(), 
							transactionID, 
							request.getSequenceNumber(),  
							null, 
							request.getData());
					log.addLog(newRecord);

					// check to see if you can commit:
					//  server has previously received a commit request and was unable to commit due to missing sequence numbers, 
					//  the server can commit if this write has satisfied all of the servers ASK_RESEND requests
					if (log.hasReceivedCommitRequest() && log.getMissingSequenceNumbers(log.getSequenceNumber()).isEmpty()) {
//						newRecord = new LogRecord (
//								RequestMessage.RequestMethod.COMMIT, 
//								transactionID, 
//								log.getSequenceNumber(), 
//								null, 
//								null);

						String response = ClientServerProtocol.buildResponse(
								ClientServerProtocol.ResponseMethod.ACK, 
								transactionID,  // return the generated transaction ID to the client
								IGNORE, 
								ClientServerProtocol.Error.NONE, 
								0,
								null);

						log.setCommited(true);			
						log.setCommitLSN();
						log.setSequenceNumber(log.getLargestSequenceNumber());
						FileServer.processCommits();

						if (FileServer.isPrimary() && !log.hasReceivedBackupACK()) {
							backupSYNC(log);
						} 
						//FileServer.addCommit(log.getTransactionID());
						//FileServer.addLog (newRecord); // since the sequence number has already been used, we just add straight to log and flush to disk 
						
						out.print(response); 
						out.flush();

					}

				}

				break;
				default:
					break;
				}	
			}
		} catch (ServerException e) {
			String response = ClientServerProtocol.buildResponse(
					ClientServerProtocol.ResponseMethod.ERROR, 
					IGNORE, 
					IGNORE, 
					e.getError(), 
					(e.getMessage().length() + e.getError().toString().length() + 2),
					e.getError().toString() + ": " + e.getMessage());
			out.print(response);
			out.flush();

		} 
		catch (SocketTimeoutException e) {
			System.err.println("No more data coming from client, server timed out after " + FileServer.getTimeout() + "ms : " + e.getMessage());

		} 
		catch (IOException e) {
			e.printStackTrace();
			System.out.print("IOException: " + e.getMessage());
		}
	}



}