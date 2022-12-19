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
				case -1:// Error
					System.err.println("Client thread encountered errors");
				case 1: // Disconnect
					running = false;
					break;
				case 0:// NOP
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
					int chunk_size = bb.getInt();
					String chunk_name = new String(chunk);
					byte[] data = new byte[chunk_size];
					readin = 0;
					while (readin != chunk_size) {
						readin += inputStream.read(data, readin, chunk_size - readin);
					}
					File file = new File(chunk_name);
					if (file.createNewFile()) {
						try (FileOutputStream fileOut = new FileOutputStream(file)) {
							fileOut.write(data);
							outputStream.write(0);
						}
					} else {
						outputStream.write(2);
					}
					outputStream.write(1);
					break;
				}
				case 4:// get file chunk or return -1
				{
					byte[] chunk = new byte[64];
					int readin = 0;
					while (readin != 64) {
						readin += inputStream.read(chunk, readin, 64 - readin);
					}
					String chunkName = new String(chunk);
					File file = new File(chunkName);
					if (file.exists()) {
						byte[] data = new byte[(int) file.length()];
						try (FileInputStream fileIn = new FileInputStream(file)) {
							fileIn.read(data, 0, (int) file.length());
						}
						outputStream
								.write(ByteBuffer.allocate(4).putInt((int) file.length()).array());
						outputStream.write(data, 0, (int) file.length());
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
					File file = new File(new String(chunk));
					if (file.delete()) {
						outputStream.write(0);
					} else {
						outputStream.write(1);
					}
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
