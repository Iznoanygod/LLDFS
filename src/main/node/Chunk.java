package main.node;

import java.io.*;
import java.nio.ByteBuffer;

public class Chunk {
    private String chunkName;
    private String fileLocation;
    private File fileSocket;
    private int size;

    public Chunk(String name, int size) {
        chunkName = name;
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    public boolean verifyChunk() {
        return true;
    }

    public int write(byte[] data) {
        File chunkDirectory = new File("chunk");
        chunkDirectory.mkdir();
        fileLocation = "chunk/" + chunkName;
        fileSocket = new File(fileLocation);
        try {
            if (fileSocket.createNewFile()) {
                try (FileOutputStream fileOut = new FileOutputStream(fileSocket)) {
                    fileOut.write(data, 0, size);
                    return 0;
                }
            } else {
                return 0;
            }
        } catch (IOException e) {
            return -1;
        }
    }

    public int destroy() {
        if (fileSocket.delete()) {
            return 1;
        } else {
            return 0;
        }
    }

    public byte[] getData() {
        byte[] data = new byte[size];
        try (FileInputStream fileIn = new FileInputStream(fileSocket)) {
            fileIn.read(data, 0, (int) size);
        } catch (IOException e) {
            return null;
        }
        return data;
    }

}
