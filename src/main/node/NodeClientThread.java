package main.node;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import main.ScalableFileSystem;

public class NodeClientThread extends Thread {
	private Socket socket;
	private InputStream inputStream;
	private OutputStream outputStream;
	private boolean running;
	
	public NodeClientThread(Socket socket) {
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
				case 2:
				default:
					System.err.println("Unknown command");
				}
			}
			inputStream.close();
			outputStream.close();
			socket.close();
		} catch (IOException e) {
			System.err.println("Client thread disconnected unexpectedly");
			running = false;
		} finally {
			ScalableFileSystem.server.removeThread(this);
		}
	}
	
	public void slowStop() {
		running = false;
	}
}
