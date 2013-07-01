package org.springframework.xd.dirt.yarn;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.integration.MessagingException;
import org.springframework.xd.dirt.launcher.RedisContainerLauncher;
import org.springframework.xd.dirt.server.AdminMain;
import org.springframework.xd.dirt.server.AdminOptions;
import org.springframework.xd.dirt.server.ContainerOptions;
import org.springframework.xd.dirt.server.Transport;
import org.springframework.xd.dirt.yarn.hmon.HBSlaveArgs;
import org.springframework.xd.dirt.yarn.hmon.HeartbeatSlave;
import org.springframework.xd.dirt.yarn.hmon.NodeInfo;
import org.springframework.xd.dirt.yarn.hmon.NodeType;

public class DirtDaemon {

	public static SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	private static ExecutorService tp;
	
	private static HeartbeatSlave hbSlave;
	
	private static volatile NodeInfo nodeInfo = new NodeInfo();
	
	public static void main(String[] args) {
		tp = Executors.newCachedThreadPool();
		try {
			if (args[0].contains("admin")) {
				startAdmin(args);
			} else {
				startContainer(args);
			}
			boolean isFinished = false;
			do {
				isFinished = tp.awaitTermination(5, TimeUnit.MINUTES);				
				System.out.println("[" + df.format(Calendar.getInstance().getTime()) + "] Daemon still running !!");
			} while (!isFinished);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void startAdmin(final String[] args)
			throws InterruptedException {
		HBSlaveArgs adminHbSlaveArgs = createHbSlaveArgs(args, NodeType.ADMIN);
		hbSlave = new HeartbeatSlave(adminHbSlaveArgs);
		hbSlave.init();
		tp.submit(hbSlave);
		tp.submit(new Runnable() {
			@Override
			public void run() {
				DirtArgs dirtArgs = new DirtArgs();		
				Tools.parseArgs(dirtArgs, args);
				boolean gotPort = false;
				int finalPort = dirtArgs.minPort;
				for (finalPort = dirtArgs.minPort; finalPort <= dirtArgs.maxPort; finalPort++) {
					AdminOptions adminOptions = new AdminOptions();
					adminOptions.setHttpPort(finalPort);
					adminOptions.setRedisHost(dirtArgs.redisHost);
					adminOptions.setRedisPort(Integer.toString(dirtArgs.redisPort));
					adminOptions.setTransport(Transport.redis);
					adminOptions.setXdHomeDir(dirtArgs.xdDir);
					AdminMain.setSystemProps(adminOptions);
					try {
						System.out.println("[" + df.format(Calendar.getInstance().getTime()) + "] Trying to start StreamServer on port [" + finalPort + "]");
						AdminMain.launchStreamServer(adminOptions);
						gotPort = true;
					} catch (Exception e) {
						System.out.println("[" + df.format(Calendar.getInstance().getTime()) + "] Could not start StreamServer on port [" + finalPort + "]");
					}
					
					if (gotPort) {
						System.out.println("[" + df.format(Calendar.getInstance().getTime()) + "] Started StreamServer on port [" + finalPort + "]");
						break;
					}
				}
				
				if (gotPort) {
					nodeInfo.setAuxEndPointPort1(finalPort);
					hbSlave.setNodeInfo(nodeInfo);
				} else {
					System.exit(-1);
				}
			}});		
	}
	
	public static void startContainer(final String[] args)
			throws InterruptedException {
		HBSlaveArgs containerHbSlaveArgs = createHbSlaveArgs(args, NodeType.CONTAINER);
		hbSlave = new HeartbeatSlave(containerHbSlaveArgs);
		hbSlave.init();
		tp.submit(hbSlave);
		tp.submit(new Runnable() {
			@Override
			public void run() {
				DirtArgs dirtArgs = new DirtArgs();		
				Tools.parseArgs(dirtArgs, args);
				int finalPort = dirtArgs.minPort;
					
				ContainerOptions conOptions = new ContainerOptions();
				conOptions.setRedisHost(dirtArgs.redisHost);
				conOptions.setRedisPort(Integer.toString(dirtArgs.redisPort));
				conOptions.setTransport(Transport.redis);
				conOptions.setXdHomeDir(dirtArgs.xdDir);
				RedisContainerLauncher.main();

				nodeInfo.setAuxEndPointPort1(finalPort);
				hbSlave.setNodeInfo(nodeInfo);
			}});
	}
	
	private static HBSlaveArgs createHbSlaveArgs(String[] args, NodeType nType) {
		HBSlaveArgs hbSlaveArgs = new HBSlaveArgs();		
		Tools.parseArgs(hbSlaveArgs, args);
		hbSlaveArgs.nodeType = nType;
		System.out.println("HB Master port : " + hbSlaveArgs.masterPort);
		return hbSlaveArgs;
	}	

}
