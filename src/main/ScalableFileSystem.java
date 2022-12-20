package main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Properties;
import main.master.MasterServer;
import main.node.NodeServer;

public class ScalableFileSystem {

	//Some constants
	public static final int CHUNK_SIZE = 32768;
	public static final int CACHE_CHUNK_SIZE = 16384;

	protected static Properties properties;
	public static Server server;

	public static void main(String[] args) {
		String ip;
		String masterip = null;
		int masterport = 0;
		int port = 0;
		//Add new server node handling here somewhere
		String configFilePath = "config.prop";
		FileInputStream propFile;
		try {
			propFile = new FileInputStream(configFilePath);
			properties = new Properties();
			properties.load(propFile);
			propFile.close();
		} catch (IOException e) {
			System.err.println("Error: Failed to load config.prop file");
			System.exit(0);
		}
		try {
			port = Integer.parseInt(properties.getProperty("PORT", "50032"));
		} catch(NumberFormatException e) {
			System.err.println("Error: Port not a number");
			System.exit(0);
		}
		ip = properties.getProperty("IP");
		boolean isMaster = Boolean.parseBoolean(properties.getProperty("MASTER"));
		if(isMaster) {
			try {
				server = new MasterServer(port);
				server.start();
				server.join();
			} catch (InterruptedException e) {
				System.err.println("Error: Thread Interrupted");
				System.exit(0);
				Thread.currentThread().interrupt();
			}
			
		}
		else {
			String masterServer = properties.getProperty("MASTERIP");
			Socket masterConnectSocket = null;
			try {
				String[] split = masterServer.split(":");
				masterip = split[0];
				
				masterport = Integer.parseInt(split[1]);
				SocketAddress addr = new InetSocketAddress(masterip, masterport);
				masterConnectSocket = new Socket();
				masterConnectSocket.connect(addr, 5000);

				OutputStream oStream = masterConnectSocket.getOutputStream();
				InputStream iStream = masterConnectSocket.getInputStream();
				oStream.write(3);

				byte[] address = InetAddress.getByName(ip).getAddress();
				byte[] portB = ByteBuffer.allocate(4).putInt(port).array();
				oStream.write(address, 0, 4);
				oStream.write(portB, 0, 4);
				int status = iStream.read();
				if(status != 0)
					throw new IOException("Error: Node rejected");
			}  catch(ArrayIndexOutOfBoundsException e) {
				System.err.println("Error: Failed to connect to any master servers");
				System.exit(0);
			} catch(IOException e) {
				System.err.println("Error: Failed to connect to " + masterip + ":" + masterport);
				System.exit(0);
			} catch(NumberFormatException e) {
				System.err.println("Error: Port not a number");
				System.exit(0);
			}
			
			System.err.println("Log: Connected to server " + masterip + ":" + masterport);
			try {
				server = new NodeServer(port, masterConnectSocket);
				server.start();
				server.join();
			}catch (InterruptedException e) {
				System.err.println("Error: Thread Interrupted");
				System.exit(0);
				Thread.currentThread().interrupt();
			}
		}
		
	}
	
	public static void closeSilently(Socket s) {
		if(s != null) {
			try {
				s.close();
			} catch(IOException e) {
				System.err.println("Warning: socket didn't close correctly, but this is not problematic");
			}
		}
	}
	
}
