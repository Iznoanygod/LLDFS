package main.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import main.ScalableFileSystem;

public class NodeThread extends Thread {
	private Socket socket;
	private InputStream inputStream;
	private OutputStream outputStream;
	private boolean running;
	private Timer scheduledTasks;
	private BlockingQueue<Runnable> taskQueue;
	private Node node;

	public NodeThread(Socket socket, Node node) {
		this.node = node;
		taskQueue = new LinkedBlockingQueue<>();

		TimerTask scheduledTask = new TimerTask() {
			@Override
			public void run() {
				taskQueue.add(() -> maintenance());
			}
		};
		Timer scheduledTasks = new Timer(true);
		scheduledTasks.scheduleAtFixedRate(scheduledTask, 5000, 60000);
		this.scheduledTasks = scheduledTasks;

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
	public void run() {// respond to node
		try {
			while (running) {
				Runnable task = taskQueue.take();
				task.run();
			}
			inputStream.close();
			outputStream.close();
			socket.close();
			scheduledTasks.cancel();
		} catch (IOException e) {
			System.err.println("Error: Node thread disconnected unexpectedly");
			running = false;
		} catch (InterruptedException e) {
			System.err.println("Error: Node thread interrupted");
			running = false;
		} finally {
			ScalableFileSystem.server.removeThread(this);
		}
	}

	public void slowStop() {
		running = false;
	}

	public BlockingQueue<Runnable> getTaskQueue() {
		return taskQueue;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public OutputStream getOutputStream() {
		return outputStream;
	}

	public void maintenance() {
		System.err.println("Log: Node thread maintenance on node " + node.getAddress() + ":" + node.getPort());
		heartBeat();
		speedTest();
	}

	public void heartBeat() {
		try {
			outputStream.write(2);
			int size = inputStream.read();
			byte[] data = new byte[size];
			int readin = 0;
			while(readin != size) {
				readin += inputStream.read(data, readin, size - readin);
			}
			//System.err.println("Log: Thread response:" + new String(data));
		} catch (IOException e) {
			System.err.println("Error: Node thread disconnected unexpectedly");
			running = false;
		}
	}
	public void speedTest() {
		try {
			outputStream.write(6);
			byte[] data = new byte[ScalableFileSystem.CHUNK_SIZE*32];
			byte[] response = new byte[ScalableFileSystem.CHUNK_SIZE*32];
			//fill data with random bytes
			for(int i = 0; i < ScalableFileSystem.CHUNK_SIZE*32; i++) {
				data[i] = (byte) (Math.random() * 256);
			}
			//start a timer
			long startTime = System.currentTimeMillis();
			outputStream.write(data, 0, ScalableFileSystem.CHUNK_SIZE*32);
			int readin = 0;
			while(readin != ScalableFileSystem.CHUNK_SIZE*32) {
				readin += inputStream.read(response, readin, ScalableFileSystem.CHUNK_SIZE*32 - readin);
			}
			long endTime = System.currentTimeMillis();
			long time = endTime - startTime;
			//System.err.println("Log: Time to send 32KB*32:" + time);
			for(int i = 0; i < ScalableFileSystem.CHUNK_SIZE*32; i++) {
				if(data[i] != response[i]) {
					System.err.println("Warning: Data mismatch at index " + i);
					break;
				}
			}
			node.setResponseTime(time);
		} catch (IOException e) {
			System.err.println("Error: Node thread disconnected unexpectedly");
			running = false;
		}
	}
}
