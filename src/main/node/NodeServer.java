package main.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import main.Server;
import main.node.NodeClientThread;

public class NodeServer extends Server{
	private Socket serverConnect;
	private Map<String,Chunk> chunkMap;
	private ServerSocket serverSocket;
	private int port;
	private boolean running;
	private Set<Thread> connectedThreads;

	private MasterThread masterThread;
	
	public NodeServer(int port, Socket server) {
		super();
		this.port = port;
		this.connectedThreads = new HashSet<>();
		this.serverConnect = server;
		MasterThread mThread = new MasterThread(server);
		connectedThreads.add(mThread);
		masterThread = mThread;
	}

	@Override
	public void run() {
		masterThread.start();
		running = true;
		try {
			serverSocket = new ServerSocket(port);
			System.err.println("Server started, listening on port " + port);
			while (running) {
				Socket clientConnect = serverSocket.accept();
				System.err.println("Comm connected");
				//Add thread, keep track of all current clients
				NodeClientThread cThread = new NodeClientThread(clientConnect);
				cThread.start();
				connectedThreads.add(cThread);
			}
			serverSocket.close();
		} catch (IOException e) {
			System.err.println("Server socket closed, shutting down");
		}
	}
	
	@Override
	public void removeThread(Thread cThread) {
		connectedThreads.remove(cThread);
	}
	
	@Override
	public void addThread(Thread cThread) {
		connectedThreads.add(cThread);
	}
	
	@Override
	public void slowStop() {
		running = false;
	}

	public void addChunk(String name, Chunk chunk){
		chunkMap.put(name, chunk);
	}

	public Chunk removeChunk(String name) {
		return chunkMap.remove(name);
	}

	public Chunk getChunk(String name) {
		return chunkMap.get(name);
	}
}
	

