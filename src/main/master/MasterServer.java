package main.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import main.Server;

public class MasterServer extends Server{
	private int port;
	private ServerSocket serverSocket;
	private boolean running;
	private Set<Thread> connectedThreads;
	
	@Override
	public void start() {
		running=true;
		super.start();
	}
	
	public MasterServer(int port, boolean master) {
		super();
		
		this.port = port;
		this.connectedThreads = new HashSet<>();
	}
	
	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(port);
			System.err.println("Server started, listening on port " + port);
			while (running) {
				Socket clientConnect = serverSocket.accept();
				System.err.println("Client connected");
				//Add thread, keep track of all current clients
				ClientThread cThread = new ClientThread(clientConnect);
				cThread.start();
				connectedThreads.add(cThread);
			}
		} catch (IOException e) {
			System.err.println("Server socket closed, shutting down");
		}
	}
	
	public void removeThread(Thread cThread) {
		connectedThreads.remove(cThread);
	}
	
	public void addThread(Thread cThread) {
		connectedThreads.add(cThread);
	}
	
	public void slowStop() {
		running = false;
	}
}
