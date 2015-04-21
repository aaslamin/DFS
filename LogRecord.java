import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LogRecord implements Serializable {
	
	private RequestMessage.RequestMethod method;
	private int transactionID;
	private boolean commited = false;
	private boolean flushed = false;
	private boolean aborted = false;
	private boolean receivedCommit = false;
	private boolean receivedBackupACK = false;
    private static final long serialVersionUID = 5950169519310163575L;
	private int sequenceNumber;
	private int commitLSN;
	private static int LSN = 0;
	private static Object mutex = new Object();
	private static Lock LSNLock = new ReentrantLock(true);
	private String filename;
	private String data;
	private HashMap<Integer, LogRecord> record;	 
	private static final String DELIMITER = "<__amir__>";
	
	
	public LogRecord (RequestMessage.RequestMethod method, int transactionID, int sequenceNumber, String filename, String data) {
		this.method = method;
		this.transactionID = transactionID;
		this.sequenceNumber = sequenceNumber;
		this.filename = filename;
		this.data = data;
		this.record = new HashMap<Integer, LogRecord>();
	}

	
	public int getCommitLSN () {
		synchronized (mutex) {
			return commitLSN;
		}
	}
	
	public static int getCurrentLSN () {
		synchronized (mutex) {
			return LSN;
		}
	}
	
	public static void setLSN (int value) {
		synchronized (mutex) {
			LSN = value;
		}
	}
	
	// utility method only usable by LogRecord 
	private static int generateCommitLSN () {
		synchronized (mutex) {
			return ++LSN;
		}
	}

	public void setCommitLSN () {
		LSNLock.lock();
		try {
			synchronized (mutex) {
				commitLSN = generateCommitLSN();
				FileServer.insertCommitLSN(this);
			}
		} finally {
			LSNLock.unlock();
		}

	}
	
	// utility method to be used only during recovery
	public void applyCommitLSN (int LSN) {
		commitLSN = LSN;
	}
	

	
	public void setCommited (boolean commited) {
		this.commited = commited;
	}
	
	public void setBackupACK (boolean hasReceived) {
		receivedBackupACK = hasReceived;
	}
	
	public boolean hasReceivedBackupACK () {
		return receivedBackupACK;
	}
	
	public boolean hasReceivedCommitRequest () {
		return receivedCommit;
	}
	
	public void setReceivedCommitRequest (boolean received) {
		receivedCommit = received;
	}
	
	public int getLargestSequenceNumber () {
		return Collections.max(record.keySet());
	}
	
	public void setFlushed (boolean flushed) {
		this.flushed = flushed;
	}
	
	public boolean hasFlushed () {
		return flushed;
	}
	
	/* returns an encoded log record to be flushed to disk */
	public String toString () {
		/* Log syntax:  <METHOD TID SEQ FLUSHED DATA> */
		StringBuilder sb = new StringBuilder();
		sb.append(method.toString());
		sb.append(DELIMITER);
		sb.append(transactionID);
		sb.append(DELIMITER);
		sb.append(sequenceNumber);

		
		switch (method) {
		case ABORT: 	 /* method<>tid<>seq */
			break;
		case COMMIT: 	 /* method<>tid<>seq<>commitLSN<>flushed */
			sb.append(DELIMITER);
			sb.append(commitLSN);
			sb.append(DELIMITER);
			sb.append(flushed);
			break;
		case NEW_TXN:	 /* method<>tid<>seq<>data */
		case WRITE:
			sb.append(DELIMITER);
			sb.append(data);
			break;
		default:
			break;
		}
		
		return sb.toString();
		//return method.toString() + DELIMITER + transactionID + DELIMITER + sequenceNumber + flushed + DELIMITER + data;
	}
	
	public static String getDecodeDelimiter () {
		return DELIMITER;
	}
	
	public void setAborted (boolean aborted) {
		this.aborted = aborted;
	}
	
	public void addLog (LogRecord newRecord) throws ServerException {
		if (newRecord.getMethod() != RequestMessage.RequestMethod.COMMIT && newRecord.getMethod() != RequestMessage.RequestMethod.ABORT && record.containsKey(newRecord.getSequenceNumber())) {
			throw new ServerException(String.format("TID: %d has already used (%d) as a sequence number. Please provide a valid sequence number. ", transactionID, newRecord.getSequenceNumber()), ClientServerProtocol.Error.INVALID_OPERATION );
		}
		
		FileServer.addLog (newRecord);
		
		switch (newRecord.getMethod()) {
		case ABORT:
			break;
		case COMMIT: 
		{
			return;
		}
		case NEW_TXN:
		{
			return;
		}
		case READ:
			break;
		case WRITE: 
		{
			//committedData.add(newRecord.getData());
		}
			break;
		default:
			break;
		}			
	
		// commit/new_txn does not need to get inserted as we can add it directly to the logfile
		record.put(newRecord.getSequenceNumber(), newRecord);
	}
	
	public boolean hasCommitted () {
		return commited;
	}
	
	public String getCommittedData () {
		StringBuilder sb = new StringBuilder();
//		for (String data : committedData) {
//			if (data != null) {
//				sb.append(data);
//			}
//		}
//		return sb.toString();
		
		for (int i = 1; i <= sequenceNumber; i++) {
			sb.append(record.get(i).getData());
		}
		
		return sb.toString();
	}

	public boolean hasAborted () {
		return aborted;
	}
	
	public ArrayList<Integer> getMissingSequenceNumbers (int maxSequenceNumber) {
		//int largestSequenceNumber = Collections.max(record.keySet());
		
		ArrayList<Integer> missingValues = new ArrayList<Integer>();
		for (int i = 1; i <= maxSequenceNumber; i++) {
			if (!record.containsKey(i)) {
				missingValues.add(i);
			}
		}
		return missingValues;
	}
	
	public boolean containsSequence (int seq) {
		if (record.containsKey(seq)) {
			return true;
		} else {
			return false;
		}
	}
	
	public RequestMessage.RequestMethod getMethod() {
		return method;
	}
	
	public void setFilename (String filename) {
		this.filename = filename;
	}

	public int getTransactionID() {
		return transactionID;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}
	
	public void setSequenceNumber (int lastNum) { // used to set the sequence number in the case of a commit (refers to the sequence number of the LAST write)
		sequenceNumber = lastNum;
	}
	
	public String getFileName ()  {
		return filename;
	}

	public String getData() {
		return data;
	}
}
