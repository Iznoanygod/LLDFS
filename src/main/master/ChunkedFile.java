package main.master;

import java.util.List;

public class ChunkedFile {
	private String file_name;
	private int size;
	private List<Chunk> chunks;

	public ChunkedFile(String file_name, int size) {
		this.file_name = file_name;
		this.size = size;
	}

	public String getFileName() {
		return file_name;
	}

	public int getSize() {
		return size;
	}

	public List<Chunk> getChunks() {
		return chunks;
	}

	public void setChunks(List<Chunk> chunks) {
		this.chunks = chunks;
	}

}
