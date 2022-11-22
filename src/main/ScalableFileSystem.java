package main;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ScalableFileSystem {

	protected static Properties properties;

	public static void main(String[] args) {
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
	}
}
