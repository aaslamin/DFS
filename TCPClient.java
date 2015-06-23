import java.net.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.text.SimpleDateFormat;

public class TCPClient
{
	//A 4KB buffer to receive server messages.
	final static int BUF_LEN = 4096; 
	final static int EXP_FIELDS = 1;
	final static int TIMEOUT = 2000;

	public static String parseCommand(String str)
	{
		String method, txn_id = "-1", seq_num = "0", content_length = "0", data = "";
		String[] result = str.split("\\s+");
		String content = "";
		String message = null;
		String header = null;

		if(result.length < EXP_FIELDS)
		{
			System.out.print("Wrong format.");
			System.out.println("A request header must have at least " +
					EXP_FIELDS + " fields");
			return null;
		}

		System.out.println("==========================================");
		for (int x=0; x<result.length; x++)
		{
			switch(x)
			{
				case 0:
					System.out.print("Method:\t\t\t");
					break;
				case 1:
					System.out.print("Txn ID:\t\t\t");
					break;
				case 2:
					System.out.print("Sequence Number:\t");
					break;
				case 3:
					System.out.print("Content Length: \t");
					break;
				case 4:
					System.out.print("Data:\t\t\t");
					break;
				default:
					System.out.print("Unknown Field:\t");
					break;
			}
			System.out.println(result[x]);
		}
		System.out.println("==========================================");

		method = result[0];
		if(result.length > 1)
			txn_id = result[1];
		if(result.length > 2)
			seq_num = result[2];
		if(result.length > 3)
			content_length = result[3];
		if(result.length > 4)
			data = result[4];


		/* Check if the method is valid */

		if(method.equals("WRITE") || method.equals("write"))
			method = "WRITE";
		if(method.equals("NEW_TXN") || method.equals("new_txn"))
			method = "NEW_TXN";
		if(method.equals("COMMIT") || method.equals("commit"))
			method = "COMMIT";
		if(method.equals("ABORT") || method.equals("abort"))
			method = "ABORT";
		if(method.equals("READ") || method.equals("read"))
			method = "READ";

		if(!method.equals("WRITE") &&
				!method.equals("NEW_TXN") &&
				!method.equals("COMMIT") &&
				!method.equals("ABORT") &&
				!method.equals("READ"))
		{
			System.out.println("Invalid method:");
			System.out.println(result[0]);
			return null;
		}

		if((new Integer(seq_num)).intValue() < 0)
		{
			System.out.println("Sequence number must be non-negative");
			return null;
		}

		if(method.equals("NEW_TXN"))
		{
			System.out.println("Looks like your method is \"NEW_TXN\"." +
					"I will generate a file name for this transaction.");
			content = "test_file.txt";
			content_length = (new Integer(content.length())).toString();
		}

		if(method.equals("WRITE"))
		{
			System.out.println("Looks like your method is \"WRITE\"." +
					"I will generate some content to send to the server.");

			Date date = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");

			content = method + " " + txn_id + " " + seq_num + " " +
				content_length + " " +
				sdf.format(date) + "\n";

			content_length = (new Integer(content.length())).toString();
		}
		if(method.equals("READ"))
		{
			System.out.println("Looks like your method is \"READ\"." +
					"I will request the file " + data + " from the server.");
			content_length = (new Integer(data.length())).toString();
			content = data;
		}

		/* Now create a string to send to the server */
		header = method + " " + txn_id + " " + seq_num + " " + content_length +
			"\r\n\r\n";

		/* If the method is not WRITE, NEW_TXN or READ, this is the message without data. It has
		 * two blank lines at the end.
		 */
		if(!method.equals("WRITE") && !method.equals("NEW_TXN") && !method.equals("READ"))
		{
			header = header + "\r\n";
		}

		message = header+content;

		System.out.println("=======================");
		System.out.println("MESSAGE SENT TO SERVER:");
		System.out.println("====> begin message");
		System.out.print(message);
		System.out.println("====> end message");

		return message;

	}

	public static void processAndSend(Socket s, String str) throws IOException
	{
		BufferedInputStream in = new BufferedInputStream(s.getInputStream());
		DataOutputStream out = new DataOutputStream(s.getOutputStream());

		byte b[] = new byte[BUF_LEN];
		int bytesRead = 0;

		String toSend = parseCommand(str);

		if(toSend == null)
			return;

		try
		{
			out.writeBytes(toSend + "\n");
		}
		catch( IOException ioe)
		{
			System.out.println("Could not send message to the server: " + ioe.getMessage());
			return;
		}
		/* Try to read a response from the server. Time out. */
		s.setSoTimeout(TIMEOUT);
		System.out.println("Received from server:");
		try
		{
			while( (bytesRead = in.read(b, 0, BUF_LEN)) > 0)
			{
				/* Display non-printable characters */
				for(int i = 0; i < bytesRead; i++)
				{
					/*
					   if(b[i] < 32 || b[i] > 126)
					   System.out.println("Unprintable character " + b[i] + ".");
					 */
				}

				b[bytesRead] = (byte)'\0';
				System.out.print(new String(b));
				bytesRead = 0;
			}
			System.out.println();
		}
		catch(SocketException se)
		{
			if(se.getMessage().equals("Connection reset"))
				return;
			else
			{
				System.out.println("Could not read from or write to the socket: " + se.getMessage());
				return;
			}

		}
		catch(SocketTimeoutException ste)
		{
			System.out.println("Timed out after " + TIMEOUT +
					"ms. No more bytes are coming from the server.");
		}
		catch(ArrayIndexOutOfBoundsException aie)
		{
			if(bytesRead < 0 )
				System.out.println("Error reading data from the server: " + aie.getMessage() );
			else
			{
				//This is most probably due to a buffer overflow.
				System.out.println("The message sent by the server is larger than the receive buffer.");
				//Replace the last byte in the buffer with the null termination character
				//and display part of the received contents
				b[(BUF_LEN - 1)] = '\0';
				System.out.println("Buffer Contents: ");
				System.out.println(new String(b));
			}
		}
		s.setSoTimeout(0);
	}

	public static void main (String args[])
	{
		Socket s = null;
		BufferedReader in;

		printHelpMessage();

		try
		{
			int serverPort = 7896;
			if(args.length >1)
			{
				try
				{
					serverPort = (new Integer(args[1])).intValue();
				}
				catch(NumberFormatException nfe)
				{
					System.out.println("Invalid port: " + args[1]);
					System.exit(0);
				}
				if(serverPort < 0 || serverPort > 65536)
				{
					System.out.println("Invalid port: " + args[1]);
					System.exit(0);
				}
			}

			String server = "localhost";
			if(args.length > 0)
				server = args[0];

			System.out.println("Hello. Will listen on " + server + ":" + serverPort);

			in = new BufferedReader(new InputStreamReader(System.in));

			/* Keep reading commands */
			while (true)
			{
				s = new Socket(server, serverPort);

				System.out.print("Enter command> ");
				String str = in.readLine();

				if(str == null)
					break;
				if(str.equals("exit"))
					break;
				if(str.equals("help"))
				{
					printHelpMessage();
					continue;
				}
				processAndSend(s, str);
				s.close();
			}

		}
		catch (UnknownHostException e)
		{
			System.out.println("Socket:"+e.getMessage());
		}
		catch (EOFException e)
		{
			System.out.println("EOF:"+e.getMessage());
		}
		catch (IOException e)
		{
			System.out.println(e.getMessage());
		}
		finally
		{
			if(s!=null)
				try
				{
					s.close();
				}
			catch (IOException e)
			{
				System.out.println("close:"+e.getMessage());
			}
			System.out.println("Exiting. Good-bye!");
		}
	}


	public static void printHelpMessage()
	{
		System.out.println("\nUse this program to send valid Assignment 2 protocol messages "
				+ "to your server.");
		System.out.println("There are two command line arguments: hostname and port.");
		System.out.println("Default values for hostname and port are: localhost:7896");
		System.out.println("The program will display a command prompt.");
		System.out.println("Enter commands at the prompt to send A2 protocol messages to " +
				"your server");
		System.out.println("To send a NEW_TXN message, type:");
		System.out.println("\t new_txn");

		System.out.println("To send a WRITE message, type:");
		System.out.println("\t write <txn_num> <seq_num>");
		System.out.println("For example:");
		System.out.println("\t write 1 1");
		System.out.println("The program will generate some content for you");

		System.out.println("To send a COMMIT message, type:");
		System.out.println("\t commit <txn_num> <seq_num>");
		System.out.println("For example:");
		System.out.println("\t commit 1 1");

		System.out.println("To send an ABORT message, type:");
		System.out.println("\t abort <txn_num> <seq_num>");
		System.out.println("For example:");
		System.out.println("\t abort 1 1");

		System.out.println("To send a READ message, type:");
		System.out.println("\t read <any txn id> <any non-neg seq_num> <any content_length> <file_name>");
		System.out.println("For example:");
		System.out.println("\t read 1 1 1 file.txt");
		System.out.println("(The correct content length will be computed automatically and send to the server.");


		System.out.println("The program will display the message sent to the server.");
		System.out.println("Then it will wait for response from the server.");
		System.out.println("It will time out after 1 second if there is no response.");
		System.out.println("You can send as many messages as you want.");
		System.out.println("To exit, type \"exit\"");
		System.out.println("To display this message again, type \"help\"\n");
	}

}
