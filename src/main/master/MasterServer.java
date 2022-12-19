package main.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import main.ScalableFileSystem;
import main.Server;
import org.apache.commons.codec.digest.DigestUtils;

public class MasterServer extends Server {
    private int port;

    private FileCache cache;
    private ServerSocket serverSocket;
    private boolean running;
    private Set<Thread> connectedThreads;
    private List<Node> nodeList;


    private Map<String, ChunkedFile> fileMap;
    private Map<String, Chunk> chunkMap;
    private BlockingQueue<Node> highSpeedNodes, lowSpeedNodes, mediumSpeedNodes;

    @Override
    public void start() {
        running = true;
        super.start();
    }

    //TODO Add scheduled tasks to handle maintenance
    public MasterServer(int port) {
        super();
        this.port = port;
        this.cache = new FileCache();
        this.connectedThreads = new HashSet<>();
        this.fileMap = new HashMap<>();
        this.chunkMap = new HashMap<>();
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

    public Chunk getMapping(String chunkName) {
        return chunkMap.get(chunkName);
    }

    public FileCache getCache() {
        return cache;
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

    public int deleteFile(String fileName) {
        if (fileMap.containsKey(fileName)) {
            //delete file
            ChunkedFile file = fileMap.remove(fileName);
            List<Chunk> chunks = file.getChunks();
            for (Chunk chunk : chunks) {
                List<Node> nodes = chunk.getNodes();
                for (Node node : nodes) {
                    //perform chunk deletion per node
                    node.deleteChunk(chunk);
                }
                chunkMap.remove(chunk.getChunkName());
            }
            return 0;
        } else {
            return 1;
        }
    }

    public int saveFile(String fileName, byte[] fileData, int size) {
        int chunkCount = (int) Math.ceil((double) size / (double) ScalableFileSystem.CHUNK_SIZE);

        List<byte[]> fileByteChunks = new ArrayList<>();
        List<String> chunkNames = new LinkedList<>();
        List<Chunk> chunks = new LinkedList<>();

        for (int i = 0; i < chunkCount; i++) {
            int chunkSize = Math.min(ScalableFileSystem.CHUNK_SIZE, size - i * ScalableFileSystem.CHUNK_SIZE);
            byte[] temp = new byte[chunkSize];
            System.arraycopy(fileData, i * ScalableFileSystem.CHUNK_SIZE, temp, 0, chunkSize);
            chunkNames.add(DigestUtils.sha256Hex(temp));
            fileByteChunks.add(temp);
        }

        if (fileMap.containsKey(fileName)) {
            ChunkedFile oldFile = fileMap.get(fileName);
            List<Chunk> oldChunks = oldFile.getChunks();
            for (Chunk chunk : oldChunks) {
                if (!chunkNames.contains(chunk.getChunkName())) {
                    //new file does not contain old chunk, delete it
                    List<Node> nodes = chunk.getNodes();
                    for (Node node : nodes) {
                        node.deleteChunk(chunk);
                        chunkMap.remove(chunk.getChunkName());
                    }
                }
            }
            for (int i = 0; i < chunkNames.size(); i++) {
                String chunkName = chunkNames.get(i);
                byte[] chunkData = fileByteChunks.get(i);
                Chunk newChunk = new Chunk(chunkName, chunkData.length);
                if (!oldChunks.contains(newChunk)) {
                    Node lowNode = lowSpeedNodes.poll();
                    Node mediumNode = mediumSpeedNodes.poll();
                    Node highNode = highSpeedNodes.poll();
                    if(lowNode != null){
                        lowNode.saveChunk(newChunk, chunkData);
                        newChunk.addNode(lowNode);
                        lowSpeedNodes.add(lowNode);
                    }
                    if(mediumNode != null) {
                        mediumNode.saveChunk(newChunk, chunkData);
                        newChunk.addNode(mediumNode);
                        mediumSpeedNodes.add(mediumNode);
                    }
                    if(highNode != null) {
                        highNode.saveChunk(newChunk, chunkData);
                        newChunk.addNode(highNode);
                        highSpeedNodes.add(highNode);
                    }
                    chunkMap.put(chunkName, newChunk);
                    chunks.add(newChunk);
                }
                else{
                    //chunk exists, so just reassign it to new file
                    Chunk oldChunk = oldChunks.get(oldChunks.indexOf(newChunk));
                    chunks.add(oldChunk);
                }
            }
        } else {
            //new file, create new chunks
            for (int i = 0; i < chunkNames.size(); i++) {
                String chunkName = chunkNames.get(i);
                byte[] chunkData = fileByteChunks.get(i);
                Chunk newChunk = new Chunk(chunkName, chunkData.length);

                Node lowNode = lowSpeedNodes.poll();
                Node mediumNode = mediumSpeedNodes.poll();
                Node highNode = highSpeedNodes.poll();
                if(lowNode != null){
                    lowNode.saveChunk(newChunk, chunkData);
                    newChunk.addNode(lowNode);
                    lowSpeedNodes.add(lowNode);
                }
                if(mediumNode != null) {
                    mediumNode.saveChunk(newChunk, chunkData);
                    newChunk.addNode(mediumNode);
                    mediumSpeedNodes.add(mediumNode);
                }
                if(highNode != null) {
                    highNode.saveChunk(newChunk, chunkData);
                    newChunk.addNode(highNode);
                    highSpeedNodes.add(highNode);
                }
                chunkMap.put(chunkName, newChunk);
                chunks.add(newChunk);
            }
        }
        ChunkedFile newFile = new ChunkedFile(fileName, size);
        newFile.setChunks(chunks);
        fileMap.put(fileName, newFile);
        return 0;
    }


}
