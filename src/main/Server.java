package main;

public abstract class Server {
	public abstract void run();
	
	public abstract void stop();
	
	public abstract void addThread(Thread cThread);
	
	public abstract void removeThread(Thread cThread);
}

