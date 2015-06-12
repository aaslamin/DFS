## Distributed File System: 

- Make sure you are using available ports prior to running the program. Prior to running the code, use the following POSIX command to check if the port you want to use is available: 
```
$ lsof -i:<port_number> 
```
- The above step is just for extra precaution, my code will do sufficient error checking
- Backup server will respond with an appropriate error message if a client attempts to connect to it
  * in such a scenario, the server will respond with the address of the primary 
- Once the server(s) are running, **do not modify** the primary file 
- When you only have the primary running and then start the backup, give the backup a few seconds to synchronize with the primary prior to crashing either server (1-2 seconds to be safe)
- Backup will detect the death of the primary within 2 seconds and will update the primary file 
- When reading the primary file, you can ignore the third value (explained below) 
- Contact me if you have any problems running the server 
	

## Syntax of the Primary File: 

[PRIMARY_IP_ADDRESS] [PRIMARY_PORT]

### Sample file: 

127.0.0.1 8080 


## How to run the server(s) via terminal (order matters): 

**(1).** Compile the source using the makefile provided:
```
$ make 
```
**(2).** Start the Primary Server:
```
$ java FileServer -ip <value> -port <value> -dir <value> -primary <FFF> -bip <XXX> -bport <YYY>
```
  * Example: 
```
$ java FileServer -dir /Users/amir/Desktop/test -primary /Users/amir/Desktop/primary.txt -bip 127.0.0.1 -bport 5938
```
**(3).** Start the Backup Server *(‘ip’ And ‘port’ __must match__ the values you provided for ‘bip’ and ‘bport’ above)*:
```
$ java FileServer -ip <XXX> -port <YYY> -dir <value> -primary <FFF>
```
  * Example: 
```
$ java FileServer -ip 127.0.0.1 -port 5938 -dir /Users/amir/Desktop/backup -primary /Users/amir/Desktop/primary.txt
```
**(4).** System is now up and running (you may begin sending transaction requests to the primary) 


### Client-Server Communication Protocol

<p>A message format similar to HTTP has been implemented. </p>
<h4>Request message</h4>
<p>A request message is a message that the client sends to the server. A client sends a request to the server for the following reasons:</p>
<ul><li>Create a new transaction.
</li><li>Read a file.
</li><li>Submit a write request that is part of an existing transaction.
</li><li>Commit an existing transaction.
</li><li>Abort a transaction.
</li></ul>
<p>A request message is to have the following format: </p>
<table><tr><th>Method</th><th>Transaction ID</th><th>Sequence number</th><th>Content Length</th><th>Data</th></tr>
<tr><td>WRITE</td><td>67681</td><td>2</td><td>13</td><td>Hello, world!</td></tr>
</table>
<p>As you can see, the message consists of several <em>fields</em>, as explained below. <em><strong> Fields are separated from one another using a single ' ' (space) character</strong></em>.</p>
<p><strong>Method Field</strong> contains the type of operations (see below for types).</p>
<p><strong>Transaction ID field</strong> specifies the ID of the transaction to which the messages relates. In the "NEW_TXN" message, transaction must be set to any id, such as "-1". The transaction ID in the "NEW_TXN" message must be ignored by the server.</p>
<p><strong>Message sequence number field</strong> identifies the number of the current file operation in the current transaction. Each transaction starts with message sequence number 0, so the "NEW_TXN" message will have sequence number 0. </p>
<p><strong>Content length</strong> field specifies the length of data (in bytes) </p>
<p><strong>Data field</strong> contains the data to be written to the file (if the method is WRITE) or the file name is the method is (NEW_TXN) or READ.</p>
<ul><li>The first four fields of the message constitute a message header.
</li><li>The request header is followed by a single blank line (a "\r\n\r\n" sequence) if the message contains the data field. The data follows that blank line.
</li><li>The request header is followed by two blank lines (a "\r\n\r\n\r\n" sequence) if the message contains no data field (as with the COMMIT method).
</li></ul>
<h4>Methods in the Request message</h4>
<p><strong>READ</strong> - the client reads the file from the server. In that case, the transaction ID can be set to any number, and must be ignored by the server, since reads are not part of any transaction. The content-length field will contain the length of the file name. The data field will contain the file name itself. For this assignment you should assume that file names contain no spaces.</p>
<p><strong>NEW_TXN</strong> - the client asks the server to begin a new transaction.</p>
<p><strong>WRITE</strong> - the client asks the server to write data as part of an existing transaction.</p>
<p><strong>COMMIT</strong> - the client asks the server to commit the transaction. In this case, the message sequence number field includes the total number of writes that were sent by the client as part of this transaction. This number should equal the sequence number of the last write, since NEW_TXN message has the sequence number of 0 and the first write has the sequence number of 1. </p>
<p><strong>ABORT</strong> - the client asks the server to abort the transaction.</p>
<h4>Response message</h4>
<p>A response is a message that the server sends to the client. A server sends the response to the client for the following reasons:</p>
<ul><li>To acknowledge a receipt of a message.
</li><li>To ask the client to resend a message that the server did not receive. 
</li><li>To report an error. 
</li></ul>
<p>A response message is to have the following format: </p>
<table><tr><th>Method</th><th>Transaction ID</th><th>Sequence number</th><th>Error code</th><th>Content Length</th><th>Data/Reason</th></tr>
<tr><td>ERROR</td><td>67681</td><td>2</td><td>201</td><td>14</td><td>Invalid TID</td></tr>
</table>
<p>Just like the request message, the response message consists of several fields and the fields are separated from one another using a single ' ' (space) character.</p>
<p><strong>Method field</strong> contains the type of operation (all methods are listed bellow)</p>
<p><strong>Transaction ID</strong> field specifies the ID of the transaction to which this message relates</p>
<p><strong>Message sequence number</strong> field in the context of response message specifies the sequence number of the message that must be retransmitted. This field only makes sense if the method is ASK_RESEND.</p>
<p><strong>Error code field</strong> specifies the error code if the method is ERROR (error codes listed below)</p>
<p><strong>Content length field</strong> specifies the length of reason (in bytes)</p>
<p><strong>Data/Reason field</strong> contains file data if the message is a response to a read request or a human readable string specifying the reason for error if the message is an error message. </p>
<ul><li>The first five fields of the message constitute a message header
</li><li>A response header is followed by two blank lines (a "\r\n\r\n\r\n" sequence) if the message does not include the "reason" or "data" field.
</li><li>If the message includes the "reason" or "data" field, the response header is followed by one blank line (a "\r\n\r\n" sequence). Data or reason follow that line. 
</li></ul>
<h4>Methods in the Response message</h4>
<p><strong> ACK </strong> - the server acknowledges the committed transaction to the client or indicates a successful response to the client's NEW_TXN request.</p>
<p><strong> ASK_RESEND </strong> - the server asks the client to resend a message for the given transaction whose sequence number is specified in the "message sequence number" field. This is in a way a "request" message, but it is send from the server to the client only in response to another message (such as a COMMIT request for which the server does not have all writes) and it will be sent over a socket opened by the client, so technically this is still a response message. </p>
<p><strong> ERROR </strong> - the server reports an error to the client. The error code and the ID of the transaction that generated the error is included in the appropriate fields in the message.</p>
<h4>Error codes:</h4>
<ul><li><strong>201</strong> - Invalid transaction ID. Sent by the server if the client had sent a message that included an invalid transaction ID, i.e., a transaction ID that the server does not remember
</li></ul>
<ul><li><strong>202</strong> - Invalid operation. Sent by the server if the client attemtps to execute an invalid operation - i.e., write as part of a transaction that had been committed
</li></ul>
<ul><li><strong>204</strong> - Wrong message format. Sent by the server if the message sent by the client does not follow the specified message format
</li></ul>
<ul><li><strong>205</strong> - File I/O error
</li></ul>
<ul><li><strong>206</strong> - File not found
</li></ul>
<h3>Example messages</h3>
<p>An example write request: </p>
<pre>WRITE 35551 1 35

Here is my data that goes into file
</pre>
<p>An example commit request message:</p>
<pre>COMMIT 35551 8 0</pre>

### How to gracefully kill the server via terminal: 

CTRL-C 

*Note: you will see a message printed on the terminal to confirm the server has been gracefully killed.* 
A graceful kill will delete _all_ files that were generated by the server except the files created from committed transactions.

### How to hard kill the server via terminal: 
```
$ kill -9 [PID] 
```
A hard kill will allow you to observe the hidden files that may have been created by the server during runtime. A hard kill does not delete any files. 


## Usage: 

```
$ java FileServer -ip [ip_address_string] -port [port_number] -dir <directory_path> -primary <primary_file_path> -bip <backup_ip_address> -bport <backup_port_number>

The following options are available: 
-dir 		 (required) Absolute path to the server's filesystem (i.e. /Users/john/Desktop/tmp/) 
-primary 	 (required) Absolute path to the file that contains the address:port of the primary server 
-ip 		 IP address to bind to (default: 127.0.0.1) 
-port 		 Port number to bind to (default: 8080) 
-bip 		 IP address of the backup server (only provide this field if you are starting the primary server 
-bport 		 Port number of the backup server (only provide this field if you are starting the primary server 
```


