package main.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import main.Server;

public class MasterServer extends Server {
    private int port;
    private ServerSocket serverSocket;
    private boolean running;
    private Set<Thread> connectedThreads;
    private List<Node> nodeList;

    private Map<String, ChunkedFile> fileMap;
    private BlockingQueue<Node> highSpeedNodes, lowSpeedNodes, mediumSpeedNodes;

    @Override
    public void start() {
        running = true;
        super.start();
    }

    public MasterServer(int port) {
        super();
        this.port = port;
        this.connectedThreads = new HashSet<>();
        this.nodeList = new LinkedList<>();
        this.lowSpeedNodes = new LinkedBlockingQueue<>();
        this.mediumSpeedNodes = new LinkedBlockingQueue<>();
        this.highSpeedNodes = new LinkedBlockingQueue<>();
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
            serverSocket.close();
        } catch (IOException e) {
            System.err.println("Server socket closed, shutting down");
        }
    }

    public Map<String, ChunkedFile> getFileMap() {
        return fileMap;
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

    public List<Node> getNodes() {
        return nodeList;
    }

    public void addNode(Node node) {
        nodeList.add(node);
        highSpeedNodes.add(node);
    }

    public void reorganizeNodes() {
        highSpeedNodes.clear();
        mediumSpeedNodes.clear();
        lowSpeedNodes.clear();
        for (int i = 0; i < nodeList.size() / 3; i++) {
            lowSpeedNodes.add(nodeList.get(i));
        }
        for (int i = nodeList.size() / 3; i < nodeList.size() * 2 / 3; i++) {
            mediumSpeedNodes.add(nodeList.get(i));
        }
        for (int i = nodeList.size() * 2 / 3; i < nodeList.size(); i++) {
            highSpeedNodes.add(nodeList.get(i));
        }

    }

    public void saveChunk(byte[] chunk){

    }

}
