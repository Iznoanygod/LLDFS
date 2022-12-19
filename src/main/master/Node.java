package main.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;

public class Node {

	private InetAddress address;

	private int port;

	private long responseTime;
	private long lastUsed;
	private NodeThread thread;


	
	public Node(InetAddress address, int port) {
		this.address = address;
		this.port = port;
	}
	
	public InetAddress getAddress() {
		return address;
	}
	
	public int getPort() {
		return port;
	}

	public void setThread(NodeThread thread) {
		this.thread = (NodeThread) thread;
	}

	public NodeThread getThread() {
		return thread;
	}

	public void setResponseTime(long responseTime) {
		this.responseTime = responseTime;
	}

	public long getResponseTime() {
		return responseTime;
	}

	public void saveChunk(Chunk chunk, byte[] data) {
		//add save chunk task to thread
		Runnable task = () -> {
			//save chunk
			OutputStream outputStream = thread.getOutputStream();
			InputStream inputStream = thread.getInputStream();
			try {
				outputStream.write(3);
				outputStream.write(chunk.getChunkName().getBytes(), 0, 64);
				outputStream
						.write(ByteBuffer.allocate(4).putInt((int) chunk.getSize()).array());
				outputStream.write(data, 0, (int) chunk.getSize());
				int status = inputStream.read();
				if (status != 0) {
					System.err.println("Warning: Node server encountered an error saving chunk, but this is not fatal as long as other nodes are available");
				}
			} catch (IOException e) {
				System.err.println("Error: Node server unexpectedly closed");
				thread.slowStop();
			}
		};

		thread.getTaskQueue().add(task);
	}

	public void deleteChunk(Chunk chunk) {
		//add delete chunk task
		Runnable task = () -> {
			//delete chunk
			OutputStream outputStream = thread.getOutputStream();
			InputStream inputStream = thread.getInputStream();
			try {
				outputStream.write(5);
				outputStream.write(chunk.getChunkName().getBytes(), 0, 64);
				int status = inputStream.read();
				if(status != 0) {
					System.err.println("Warning: Node server encountered an error deleting chunk, but this is not fatal");
				}
			} catch (IOException e) {
				System.err.println("Error: Node server unexpectedly closed");
				thread.slowStop();
			}
		};

		thread.getTaskQueue().add(task);
	}
}
