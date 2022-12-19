package main.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import main.ScalableFileSystem;

public class ClientThread extends Thread {

	private Socket socket;
	private InputStream inputStream;
	private OutputStream outputStream;
	private boolean running;
	private boolean preserveSocket;
	
	public ClientThread(Socket socket) {
		preserveSocket = false;
		try {
			this.socket = socket;
			this.inputStream = socket.getInputStream();
			this.outputStream = socket.getOutputStream();
		
			running = true;
		} catch(IOException e) {
			System.err.println("Failed to create socket");
			running = false;
		}
	}
	
	@Override
	public void run() {
		try {
			while(running) {
				byte command = (byte) inputStream.read();
				switch(command) {
				case -1:
					System.err.println("Client thread encountered errors");
				case 1: //Disconnect
					running = false;
					break;
				case 0:
					break;
				case 2: //client request all node addresses
				{
					MasterServer mserver = (MasterServer) ScalableFileSystem.server;
					List<Node> nodes = mserver.getNodes();
					outputStream.write(nodes.size());
					for(int i = 0; i < nodes.size(); i++) {
						byte[] address = nodes.get(i).getAddress().getAddress();
						outputStream.write(address, 0, 4);
						byte[] port = ByteBuffer.allocate(4).putInt(nodes.get(i).getPort()).array();
						outputStream.write(port, 0, 4);
					}
					
				}
					break;
				case 3:
				{
					preserveSocket = true;
					running = false;
					NodeThread nThread = new NodeThread(socket);
					nThread.start();
					ScalableFileSystem.server.addThread(nThread);
					ScalableFileSystem.server.removeThread(this);
					break;
				}
				case 10:
				{
					running = false;
					break;
				}
				default:
					System.err.println("Unknown command:"+command);
				}
			}
			if(!preserveSocket) {
				inputStream.close();
				outputStream.close();
				socket.close();
			}
		} catch (IOException e) {
			System.err.println("Client thread disconnected unexpectedly");
			ScalableFileSystem.closeSilently(socket);
			running = false;
		} finally {
			ScalableFileSystem.server.removeThread(this);
		}
	}
	
	public void slowStop() {
		running = false;
	}
	
}
