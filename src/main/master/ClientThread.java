package main.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import main.ScalableFileSystem;
import org.apache.commons.codec.digest.DigestUtils;

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
		} catch(IOException e) {
			System.err.println("Failed to create socket");
			running = false;
		}
	}
	
	@Override
	public void run() {
		try {
			while(running) {
				byte command = (byte) inputStream.read();
				switch(command) {
				case -1:
					System.err.println("Client thread encountered errors");
				case 1: //Disconnect
					running = false;
					break;
				case 0:
					break;
				case 2: //client request all node addresses
				{
					MasterServer mserver = (MasterServer) ScalableFileSystem.server;
					List<Node> nodes = mserver.getNodes();
					outputStream.write(nodes.size());
					for(int i = 0; i < nodes.size(); i++) {
						byte[] address = nodes.get(i).getAddress().getAddress();
						outputStream.write(address, 0, 4);
						byte[] port = ByteBuffer.allocate(4).putInt(nodes.get(i).getPort()).array();
						outputStream.write(port, 0, 4);
					}
					
				}
					break;
				case 3://convert client to node
				{
					int readin = 0;
					byte[] addressB = new byte[4];
					while(readin != 4) {
						readin += inputStream.read(addressB, readin, 4 - readin);
					}
					byte[] port = new byte[4];
					readin = 0;
					while(readin != 4) {
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
					((MasterServer)ScalableFileSystem.server).addNode(temp);
					System.err.println("Client converted to node");
					outputStream.write(0);
					break;
				}
					case 4://save file
					{//client asks to save a file
						MasterServer mServer = (MasterServer)ScalableFileSystem.server;
						int readin = 0;
						byte[] fileNameSize = new byte[4];
						while(readin != 4) {
							readin += inputStream.read(fileNameSize, readin, 4 - readin);
						}
						int fileNameSizeInt = ByteBuffer.wrap(fileNameSize).getInt();
						byte[] fileName = new byte[fileNameSizeInt];
						readin = 0;
						while(readin != fileNameSizeInt) {
							readin += inputStream.read(fileName, readin, fileNameSizeInt - readin);
						}
						String fileNameString = new String(fileName);
						readin = 0;
						byte[] fileSize = new byte[4];
						while(readin != 4) {
							readin += inputStream.read(fileSize, readin, 4 - readin);
						}
						int size = ByteBuffer.wrap(fileSize).getInt();
						byte[] file = new byte[size];
						readin = 0;
						while(readin != size) {
							readin += inputStream.read(file, readin, size - readin);
						}
						int chunkCount = (int) Math.ceil((double)size / (double)ScalableFileSystem.CHUNK_SIZE);

						byte[][] fileByteChunks = new byte[chunkCount][];
						String[] chunkNames = new String[chunkCount];

						for(int i = 0; i < chunkCount; i++){
							int chunkSize = Math.min(ScalableFileSystem.CHUNK_SIZE, size - i * ScalableFileSystem.CHUNK_SIZE);
							fileByteChunks[i] = new byte[chunkSize];
							System.arraycopy(file, i * ScalableFileSystem.CHUNK_SIZE, fileByteChunks[i], 0, chunkSize);
							chunkNames[i] = DigestUtils.sha256Hex(fileByteChunks[i]);
						}
						//check if file already exists
						if(mServer.getFileMap().containsKey(fileNameString)) {
							//update file
							//TODO Finish this part
						}
					}
					break;
					case 5://get file
					{//client asks to get a file
						//TODO Finish this part
					}
					break;
					case 6://delete file
					{
						//client asks to delete a file
						MasterServer mServer = (MasterServer)ScalableFileSystem.server;
						int readin = 0;
						byte[] fileNameSize = new byte[4];
						while(readin != 4) {
							readin += inputStream.read(fileNameSize, readin, 4 - readin);
						}
						int fileNameSizeInt = ByteBuffer.wrap(fileNameSize).getInt();
						byte[] fileName = new byte[fileNameSizeInt];
						readin = 0;
						while(readin != fileNameSizeInt) {
							readin += inputStream.read(fileName, readin, fileNameSizeInt - readin);
						}
						String fileNameString = new String(fileName);
						if(mServer.getFileMap().containsKey(fileNameString)) {
							//delete file
							ChunkedFile file = mServer.getFileMap().remove(fileNameString);
							List<Chunk> chunks = file.getChunks();
							for(Chunk chunk : chunks) {
								List<Node> nodes = chunk.getNodes();
								for(Node node : nodes) {
									//perform chunk deletion per node
									node.deleteChunk(chunk);
								}
							}
							outputStream.write(0);
						}
						else {
							//file does not exist
							outputStream.write(1);
						}
					}
					break;
				case 10:
				{
					running = false;
					break;
				}
				default:
					System.err.println("Unknown command:"+command);
				}
			}
			if(!preserveSocket) {
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
