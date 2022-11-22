package main;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class ScalableFileSystem {

	protected static Properties properties;

	public static void main(String[] args) {
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
		}
		String[] masterServers = properties.getProperty("MASTER").split(",");
		try {
			int port = Integer.parseInt(properties.getProperty("PORT", "50032"));
		} catch(NumberFormatException e) {
			System.err.println("Port not a number");
		}
		Socket masterConnectSocket;
		String ip = null;
		int port = 0;
		try {
			for(int i = 0;;i++) {
				String[] split = masterServers[i].split(":");
				ip = split[0];
				try {
					port = Integer.parseInt(split[1]);
				} catch(NumberFormatException e) {
					System.err.println("Port not a number");
					System.exit(0);
				}
				try {
					SocketAddress addr = new InetSocketAddress(ip, port);
					Socket tempSocketConnect = new Socket();
					tempSocketConnect.connect(addr, 1000);
					masterConnectSocket = tempSocketConnect;
					Thread.sleep(1000000);
					break;
				} catch(Exception e) {
					System.err.println("Failed to connect to " + ip + ":" + port);
				}
			}
		} catch(ArrayIndexOutOfBoundsException e) {
			System.err.println("Failed to connect to any master servers");
			System.exit(0);
		} 
		System.err.println("Connected to server " + ip + ":" + port);
	}
}
