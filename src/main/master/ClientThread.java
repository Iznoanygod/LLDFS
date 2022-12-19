package main.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
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
        } catch (IOException e) {
            System.err.println("Failed to create socket");
            running = false;
        }
    }

    @Override
    public void run() {
        try {
            while (running) {
                byte command = (byte) inputStream.read();
                switch (command) {
                    case -1:
                        System.err.println("Client thread encountered errors");
                    case 1: //Disconnect
                        outputStream.write(0);
                        slowStop();
                        break;
                    case 0:
                        break;
                    case 2: //client request all node addresses
                    {
                        MasterServer mserver = (MasterServer) ScalableFileSystem.server;
                        List<Node> nodes = mserver.getNodes();
                        outputStream.write(nodes.size());
                        for (Node node : nodes) {
                            byte[] address = node.getAddress().getAddress();
                            outputStream.write(address, 0, 4);
                            byte[] port = ByteBuffer.allocate(4).putInt(node.getPort()).array();
                            outputStream.write(port, 0, 4);
                        }

                    }
                    break;
                    case 3://convert client to node
                    {
                        int readin = 0;
                        byte[] addressB = new byte[4];
                        while (readin != 4) {
                            readin += inputStream.read(addressB, readin, 4 - readin);
                        }
                        byte[] port = new byte[4];
                        readin = 0;
                        while (readin != 4) {
                            readin += inputStream.read(port, readin, 4 - readin);
                        }
                        InetAddress iaddress = InetAddress.getByAddress(addressB);
                        ByteBuffer bb = ByteBuffer.wrap(port);
                        int portnum = bb.getInt();
                        preserveSocket = true;
                        running = false;
                        Node temp = new Node(iaddress, portnum);
                        NodeThread nThread = new NodeThread(socket, temp);
                        temp.setThread(nThread);
                        nThread.start();
                        ScalableFileSystem.server.addThread(nThread);
                        ScalableFileSystem.server.removeThread(this);
                        ((MasterServer) ScalableFileSystem.server).addNode(temp);
                        System.err.println("Client converted to node");
                        outputStream.write(0);
                        break;
                    }
                    case 4://save file
                    {//client asks to save a file
                        MasterServer mServer = (MasterServer) ScalableFileSystem.server;
                        int readin = 0;
                        byte[] fileNameSize = new byte[4];
                        while (readin != 4) {
                            readin += inputStream.read(fileNameSize, readin, 4 - readin);
                        }
                        int fileNameSizeInt = ByteBuffer.wrap(fileNameSize).getInt();
                        byte[] fileName = new byte[fileNameSizeInt];
                        readin = 0;
                        while (readin != fileNameSizeInt) {
                            readin += inputStream.read(fileName, readin, fileNameSizeInt - readin);
                        }
                        String fileNameString = new String(fileName);
                        readin = 0;
                        byte[] fileSize = new byte[4];
                        while (readin != 4) {
                            readin += inputStream.read(fileSize, readin, 4 - readin);
                        }
                        int size = ByteBuffer.wrap(fileSize).getInt();
                        byte[] file = new byte[size];
                        readin = 0;
                        while (readin != size) {
                            readin += inputStream.read(file, readin, size - readin);
                        }
                        int status = mServer.saveFile(fileNameString, file, size);
                        outputStream.write(status);
                        break;
                    }
                    case 5://get file
                    {//client asks to get a file
                        //TODO Finish this part
                        int readin = 0;
                        byte[] fileNameSize = new byte[4];
                        while (readin != 4) {
                            readin += inputStream.read(fileNameSize, readin, 4 - readin);
                        }
                        int fileNameSizeInt = ByteBuffer.wrap(fileNameSize).getInt();
                        byte[] fileName = new byte[fileNameSizeInt];
                        readin = 0;
                        while (readin != fileNameSizeInt) {
                            readin += inputStream.read(fileName, readin, fileNameSizeInt - readin);
                        }
                        String fileNameString = new String(fileName);
                        MasterServer mServer = (MasterServer) ScalableFileSystem.server;
                        if (mServer.getFileMap().containsKey(fileNameString)) {
                            outputStream.write(0);
                            ChunkedFile file = mServer.getFileMap().get(fileNameString);
                            outputStream.write(file.getChunks().size());
                            for (Chunk chunk : file.getChunks()) {
                                outputStream.write(chunk.getChunkName().getBytes(), 0, 64);
                            }
                        } else {
                            outputStream.write(1);
                        }

                    }
                    break;
                    case 6://delete file
                    {
                        //client asks to delete a file
                        MasterServer mServer = (MasterServer) ScalableFileSystem.server;
                        int readin = 0;
                        byte[] fileNameSize = new byte[4];
                        while (readin != 4) {
                            readin += inputStream.read(fileNameSize, readin, 4 - readin);
                        }
                        int fileNameSizeInt = ByteBuffer.wrap(fileNameSize).getInt();
                        byte[] fileName = new byte[fileNameSizeInt];
                        readin = 0;
                        while (readin != fileNameSizeInt) {
                            readin += inputStream.read(fileName, readin, fileNameSizeInt - readin);
                        }
                        String fileNameString = new String(fileName);
                        int status = mServer.deleteFile(fileNameString);
                        outputStream.write(status);
                    }
                    break;
                    case 7://get chunk
                    {
                        MasterServer mServer = (MasterServer) ScalableFileSystem.server;
                        int readin = 0;
                        byte[] chunkName = new byte[64];
                        while (readin != 64) {
                            readin += inputStream.read(chunkName, readin, 64 - readin);
                        }
                        String chunkNameString = new String(chunkName);
                        Chunk chunk = mServer.getMapping(chunkNameString);
                        if (mServer.getCache().hasChunk(chunkNameString)) {
                            outputStream.write(0);
                            outputStream.write(ByteBuffer.allocate(4).putInt(chunk.getSize()).array());
                            outputStream.write(mServer.getCache().getChunk(chunkNameString), 0, chunk.getSize());
                        } else {
                            outputStream.write(1);
                            outputStream.write(chunk.getNodes().size());
                            for (Node node : chunk.getNodes()) {
                                outputStream.write(node.getAddress().getAddress(), 0, 4);
                                outputStream.write(ByteBuffer.allocate(4).putInt(node.getPort()).array());
                            }
                            //retrieve chunk and put it in the cache
                            List<Node> nodes = chunk.getNodes();
                            //find node with lowest response time
                            Node bestNode = nodes.get(0);
                            long bestTime = Long.MAX_VALUE;
                            for (Node node : nodes) {
                                long time = node.getResponseTime();
                                if (time < bestTime) {
                                    bestTime = time;
                                    bestNode = node;
                                }
                            }
                            final Node finalBestNode = bestNode;
                            Runnable cacheFetch = () -> {
                                try {
                                    outputStream.write(4);
                                    int readIn = 0;
                                    byte[] fileSize = new byte[4];
                                    while (readIn != 4) {
                                        readIn += inputStream.read(fileSize, readIn, 4 - readIn);
                                    }
                                    int size = ByteBuffer.wrap(fileSize).getInt();
                                    if(size == -1) {
                                        System.err.println("Warning: node did not have expected chunk");
                                        nodes.remove(finalBestNode);
                                        return;
                                    }

                                    byte[] chunkData = new byte[size];
                                    readIn = 0;
                                    while (readIn != size) {
                                        readIn += inputStream.read(chunkData, readIn, size - readIn);
                                    }
                                    mServer.getCache().addChunk(chunkNameString, chunkData);
                                } catch (IOException e) {
                                    System.err.println("Error: node disconnected");
                                    running = false;
                                }
                            };
                            bestNode.getThread().getTaskQueue().add(cacheFetch);
                            //retrieval finished
                        }
                    }
                    break;
                    case 10: {
                        running = false;
                        break;
                    }
                    default:
                        System.err.println("Unknown command:" + command);
                }
            }
            if (!preserveSocket) {
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
