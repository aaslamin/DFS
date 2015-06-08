import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class Client extends Thread {
	
	private static InetAddress serverAddress;
	private static int serverPort;
	private static String filename; // the file that all clients will be writing to
	private static int totalWrites = 20;
	private static Lock mutexLock = new ReentrantLock();
	public static ArrayList<Integer> commitOrder = new ArrayList<Integer>();
	
	private int transactionID;
	private Socket socket;
	private DataOutputStream out;
	private String dataToWrite;
	private StringBuilder committedData = new StringBuilder();
	
	private static final CountDownLatch startBarrier = new CountDownLatch(ConcurrencyTest.getTotalClients());
	
	public Client (String address, int port, String file, String data, int tid) {
		
		try {
			serverAddress = InetAddress.getByName(address);
		} catch (UnknownHostException e) {
			System.err.println("Invalid server address: " + serverAddress);
			System.exit(1);
		}
		serverPort = port;
		filename = file;
		dataToWrite = data;
		transactionID = tid;
		try {
			socket = new Socket(serverAddress, serverPort);
			socket.setTcpNoDelay(true);
			out = new DataOutputStream(socket.getOutputStream());
		} catch (IOException e) {
			System.err.println("IOException: " + e.getMessage());
			System.exit(1);;
		}
		
	}
	
	@Override
	public void run () {
		
		// all clients will start at the same time
		startBarrier.countDown();
		try {
			startBarrier.await();
		} catch (InterruptedException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		String request;
		for (int i = 1; i <= totalWrites; i++) {
			request = ConcurrencyTest.buildRequest(
					ConcurrencyTest.RequestMethod.WRITE, 
					transactionID, 
					i, 
					dataToWrite.length(), 
					dataToWrite);
			try {
				out.writeBytes(request);
				out.flush();
				out.flush();
				committedData.append(dataToWrite);
				try {
					Thread.currentThread().sleep(10);
					//socket.close();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			} catch (IOException e) {
				System.err.println("Could not send request to server: " + e.getMessage() + " Thread ID: " + Thread.currentThread().getId());
				System.exit(1);			
			}
		}
		

		
		request = ConcurrencyTest.buildRequest(
				ConcurrencyTest.RequestMethod.COMMIT, 
				transactionID, 
				totalWrites, 
				0, 
				null);
		
		try {

			synchronized (mutexLock) {
				// commit transaction to the server
				out.writeBytes(request);
				out.flush();
				ConcurrencyTest.appendData(committedData.toString());
				commitOrder.add(transactionID);
				try {
					Thread.currentThread().sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			ConcurrencyTest.endBarrier.countDown();
			
		} catch (IOException e) {
			System.err.println("Could not send request to server: " + e.getMessage() + " Thread ID: " + Thread.currentThread().getId());
			System.exit(1);
		}
		
		finally {
			
			try {
				Thread.currentThread().sleep(200);
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
