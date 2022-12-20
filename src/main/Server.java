package main;

public abstract class Server extends Thread {
	public abstract void run();
	
	public abstract void slowStop();
	
	public abstract void addThread(Thread cThread);
	
	public abstract void removeThread(Thread cThread);
}

