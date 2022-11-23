package main.node;

import java.io.File;

public class Chunk {
	private String chunkName;
	private String fileLocation;
	private File fileSocket;
	private long size;
	
	public long getSize() {
		return size;
	}
	
	public boolean verifyChunk() {
		return true;
	}
}
