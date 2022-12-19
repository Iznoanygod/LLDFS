package main.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

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
                        running = false;
                        break;
                    case 0:
                        break;
                    case 2://get file chunk or return -1
                    {
                        byte[] chunk = new byte[64];
                        int readin = 0;
                        while (readin != 64) {
                            readin += inputStream.read(chunk, readin, 64 - readin);
                        }
                        String chunkName = new String(chunk);
                        //use chunk config
                        File file = new File(chunkName);
                        if (file.exists()) {
                            byte[] data = new byte[(int) file.length()];
                            try (FileInputStream fileIn = new FileInputStream(file)) {
                                fileIn.read(data, 0, (int) file.length());

                            }
                            outputStream.write(ByteBuffer.allocate(4).putInt((int) file.length()).array());
                            outputStream.write(data, 0, (int) file.length());
                        } else {
                            outputStream.write(ByteBuffer.allocate(4).putInt(-1).array());
                        }
                        break;
                    }
                    case 3://speed test chunk
                    {
                        System.err.println("Speed test");
                        byte[] data = new byte[ScalableFileSystem.CHUNK_SIZE*32];
                        int readin = 0;
                        while (readin != ScalableFileSystem.CHUNK_SIZE*32) {
                            readin += inputStream.read(data, readin, ScalableFileSystem.CHUNK_SIZE*32 - readin);
                        }
                        outputStream.write(data, 0, ScalableFileSystem.CHUNK_SIZE*32);
                        break;
                    }
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
