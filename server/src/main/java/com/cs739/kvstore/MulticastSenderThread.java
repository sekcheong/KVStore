package com.cs739.kvstore;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;

public class MulticastSenderThread implements Runnable {
	private DatagramSocket socket;
	private InetAddress broadcastIP;
	private BlockingQueue<String> blockingQueue;
	public MulticastSenderThread(DatagramSocket socket, InetAddress broadcastIP,
			BlockingQueue<String> blockingQueue) {
		this.socket = socket;
		this.broadcastIP = broadcastIP;
		this.blockingQueue = blockingQueue;
	}
	@Override
	public void run() {
		System.out.println("Started multicast sender thread...");
		while(true) {
			String message = "";
			try {
				message = blockingQueue.take();
			} catch (Exception e) {
				e.printStackTrace();
			}
			DatagramPacket packet;
			packet = new DatagramPacket(message.getBytes(), message.length(), 
					broadcastIP, 4446);
			try {
				socket.send(packet);        
			} catch (Exception e) {
				e.printStackTrace();
			}
		}		
	}

}
