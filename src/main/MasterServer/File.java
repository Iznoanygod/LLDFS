package main.MasterServer;

import java.util.List;

public class File {
	private String file_name;
	private long size;
	private String owner;
	private short perms;
	private List chunks;
	private char[] data;
}