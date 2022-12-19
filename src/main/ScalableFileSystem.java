package main;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Properties;
import main.master.MasterServer;
import main.node.NodeServer;

public class ScalableFileSystem {

	protected static Properties properties;
	public static Server server;

	public static void main(String[] args) {
		String ip = null;
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
			System.err.println("Failed to load config.prop file");
			System.exit(0);
		}
		try {
			port = Integer.parseInt(properties.getProperty("PORT", "50032"));
		} catch(NumberFormatException e) {
			System.err.println("Port not a number");
			System.exit(0);
		}
		boolean isMaster = Boolean.parseBoolean(properties.getProperty("MASTER"));
		if(isMaster) {
			try {
				server = new MasterServer(port);
				server.start();
				server.join();
			} catch (InterruptedException e) {
				System.err.println("Thread Interrupted");
				System.exit(0);
				Thread.currentThread().interrupt();
			}
			
		}
		else {
			String masterServer = properties.getProperty("MASTERIP");
			Socket masterConnectSocket = null;
			try {
				String[] split = masterServer.split(":");
				String masterip = split[0];
				
				int masterport = Integer.parseInt(split[1]);
				SocketAddress addr = new InetSocketAddress(masterip, masterport);
				masterConnectSocket = new Socket();
				masterConnectSocket.connect(addr, 1000);
			}  catch(ArrayIndexOutOfBoundsException e) {
				System.err.println("Failed to connect to any master servers");
				System.exit(0);
			} catch(IOException e) {
				System.err.println("Failed to connect to " + ip + ":" + port);
			} catch(NumberFormatException e) {
				System.err.println("Port not a number");
				System.exit(0);
			}
			
			System.err.println("Connected to server " + ip + ":" + port);
			try {
				server = new NodeServer(port, masterConnectSocket);
				server.start();
				server.join();
			}catch (InterruptedException e) {
				System.err.println("Thread Interrupted");
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
				
			}
		}
	}
	
}
