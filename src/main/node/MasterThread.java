package main.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import main.ScalableFileSystem;

public class MasterThread extends Thread {
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private boolean running;

    public MasterThread(Socket socket) {
        try {
            this.socket = socket;
            this.inputStream = socket.getInputStream();
            this.outputStream = socket.getOutputStream();

            running = true;
        } catch (IOException e) {
            System.err.println("Error: Failed to create socket");
            running = false;
        }
    }

    @Override
    public void run() {
        try {
            while (running) {
                byte command = (byte) inputStream.read();
                switch (command) {
                    case -1:// Error
                        System.err.println("Warning: Client thread encountered errors");
                    case 1: // Disconnect
                        System.err.println("Log: Thread disconnected");
                        running = false;
                        break;
                    case 0:// NOP
                        System.err.println("Log: NOP");
                        break;
                    case 2:// heartbeat
                    {
                        String message = "alive";
                        outputStream.write(message.length());
                        outputStream.write(message.getBytes(), 0, message.length());
                        break;
                    }
                    case 3:// store file chunk
                    {
                        // read 64 bytes for chunk hash
                        byte[] chunk = new byte[64];
                        int readin = 0;
                        while (readin != 64) {
                            readin += inputStream.read(chunk, readin, 64 - readin);
                        }
                        byte[] size = new byte[4];
                        readin = 0;
                        while (readin != 4) {
                            readin += inputStream.read(size, readin, 4 - readin);
                        }
                        ByteBuffer bb = ByteBuffer.wrap(size);
                        int chunkSize = bb.getInt();
                        String chunkName = new String(chunk);
                        System.err.println("Log: Storing chunk " + chunkName);
                        Chunk fileChunk = new Chunk(chunkName, chunkSize);
                        byte[] data = new byte[chunkSize];//chunk
                        readin = 0;
                        while (readin != chunkSize) {//read chunk data
                            readin += inputStream.read(data, readin, chunkSize - readin);
                        }
                        ((NodeServer) ScalableFileSystem.server).addChunk(chunkName, fileChunk);
                        fileChunk.write(data);
                        outputStream.write(0);
                        break;
                    }
                    case 4:// get file chunk or return
                    {
                        byte[] chunk = new byte[64];
                        int readin = 0;
                        while (readin != 64) {
                            readin += inputStream.read(chunk, readin, 64 - readin);
                        }
                        String chunkName = new String(chunk);
                        Chunk fileChunk = ((NodeServer) ScalableFileSystem.server).getChunk(chunkName);
                        System.err.println("Log: Fetching chunk " + chunkName);
                        byte[] data = fileChunk.getData();
                        if (data != null) {
                            outputStream
                                    .write(ByteBuffer.allocate(4).putInt((int) fileChunk.getSize()).array());
                            outputStream.write(data, 0, (int) fileChunk.getSize());
                        } else {
                            outputStream.write(ByteBuffer.allocate(4).putInt(-1).array());
                        }
                        break;
                    }
                    case 5:// destroy file chunk
                    {
                        byte[] chunk = new byte[64];
                        int readin = 0;
                        while (readin != 64) {
                            readin += inputStream.read(chunk, readin, 64 - readin);
                        }
                        String chunkName = new String(chunk);
                        System.err.println("Log: Destroyed chunk " + chunkName);
                        Chunk fileChunk = ((NodeServer) ScalableFileSystem.server).removeChunk(chunkName);
                        fileChunk.destroy();
                        outputStream.write(ByteBuffer.allocate(4).putInt(0).array());
                    }
                    case 6://speed test node
                    {
                        System.err.println("Log: Performing speed test");
						byte[] data = new byte[ScalableFileSystem.CHUNK_SIZE*32];
						int readin = 0;
						while (readin != ScalableFileSystem.CHUNK_SIZE*32) {
							readin += inputStream.read(data, readin, ScalableFileSystem.CHUNK_SIZE*32 - readin);
						}
						outputStream.write(data, 0, ScalableFileSystem.CHUNK_SIZE*32);
                        break;
                    }

                    default:
                        System.err.println("Warning: Unknown command:"+command);
                }
            }
            inputStream.close();
            outputStream.close();
            socket.close();

        } catch (IOException e) {
            System.err.println("Error: Master thread disconnected unexpectedly");
            System.exit(0);
        } finally {
            ScalableFileSystem.server.removeThread(this);
        }
    }

    public void slowStop() {
        running = false;
    }

}
