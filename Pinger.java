import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

public class Pinger implements Runnable {

	private InetSocketAddress server;
	private int timeout;
	private static Object mutex = new Object();

	public Pinger (InetSocketAddress server, int timeout) {
		this.server = server;
		this.timeout = timeout;
	}

	@Override
	public void run() {
		Socket connection = new Socket();
		try {
			connection.setTcpNoDelay(true);
			connection.setReuseAddress(true);
		} catch (SocketException e) {
		}
		
		FileServer.setPeerStatus(server, true);
		try {
			//System.out.println(String.format("Sending heartbeat to %s:%s", server.getAddress().getHostAddress(), server.getPort()));
			connection.connect(server, timeout);
		} catch (IOException e) {
			// failed to connect to the server, remove it from the server list
			FileServer.setPeerStatus(server, false);
			synchronized (mutex) {
				if (!FileServer.isPrimary()) {
					FileServer.updatePrimary(FileServer.bindAddr, FileServer.port, FileServer.primaryFile);			
					System.out.println(String.format("[** Promoted to primary - %s:%s **]".toUpperCase(), FileServer.bindAddr.getHostAddress(), FileServer.port));
				}
			}
		
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
