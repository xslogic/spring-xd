package org.springframework.xd.dirt.yarn.controller;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.xd.dirt.yarn.AdminArgs;
import org.springframework.xd.dirt.yarn.ContainerArgs;
import org.springframework.xd.dirt.yarn.DirtArgs;
import org.springframework.xd.dirt.yarn.DirtDaemon;
import org.springframework.xd.dirt.yarn.hmon.HBMasterArgs;
import org.springframework.xd.dirt.yarn.hmon.HeartbeatMaster;
import org.springframework.xd.dirt.yarn.hmon.HeartbeatMasterClient;
import org.springframework.xd.dirt.yarn.hmon.HeartbeatNode;
import org.springframework.xd.dirt.yarn.hmon.HeartbeatNode.NodeState;
import org.springframework.xd.dirt.yarn.hmon.NodeType;
import org.springframework.xd.dirt.yarn.hmon.THeartbeatCommandEndPoint;

public class DirtController implements Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(DirtController.class);		
	
	private LinkedBlockingQueue<ClientRequest> inQ = new LinkedBlockingQueue<ClientRequest>();
	private LinkedBlockingQueue<ControllerRequest> outQ = new LinkedBlockingQueue<ControllerRequest>();
	
	private final DirtArgs dArgs;
	
	private final ControllerClient client;
	
	private volatile HeartbeatMaster hbMaster = null;	
	
	private volatile AtomicInteger numContainersRunning = new AtomicInteger(0);
	
	private volatile AtomicInteger numContainersDied = new AtomicInteger(0);
	
	private List<SlaveHandler> slaves = new ArrayList<DirtController.SlaveHandler>();
	private AdminSlaveHandler adminHandler;
	
	public DirtController(DirtArgs args, ControllerClient client) {
		this.dArgs = args;
		this.client = client;
	}
	
	/**
	 * Interface to the outside world. An external client
	 * such as the ApplicationMaster will wait on this for any
	 * new requests from the Controller
	 * @return
	 * @throws InterruptedException 
	 */
	public ControllerRequest getNextRequest() throws InterruptedException {
		return this.outQ.take();
	}
		
	public int getAdminPort() {
		if (adminHandler != null) {
			if (adminHandler.state.equals(SlaveState.NODE_RUNNING)) {
				return adminHandler.getRunningPort();
			}
		}
		return -1;
	}
	
	public String getAdminHost() {
		if (adminHandler != null) {
			if (adminHandler.state.equals(SlaveState.NODE_RUNNING)) {
				return adminHandler.getRunningHost();
			}
		}
		return "";
	}
	
	public void scaleUpBy(int numContainers) {
		this.inQ.add(new ClientRequest(numContainers));
	}
	
	public void scaleDownBy(int numContainers) {
		this.inQ.add(new ClientRequest(-1 * numContainers));
	}	
	
	public void init() {
		try {
			this.hbMaster = startHbMaster(client);
		} catch (InterruptedException e) {
			logger.error("Thread interrupted", e);
		}
		this.adminHandler = new AdminSlaveHandler(createAdminLaunchArgs(this.dArgs));
		slaves.add(adminHandler);
		for (int i = 0; i < this.dArgs.numContainers; i++) {
			slaves.add(new ContainerSlaveHandler(createContainerLaunchArgs(this.dArgs)));
		}		
	}

	@Override
	public void run() {		
				
		while (true) {
			
			// Let Admin start first
			if (this.adminHandler.getSlaveState().equals(SlaveState.NOT_STARTED)) {
				this.adminHandler.askForNode();
			}
			
			// Once Admin is running, start other nodes
			if (this.adminHandler.getSlaveState().equals(SlaveState.NODE_RUNNING)) {
				for (SlaveHandler slave : slaves) {
					if (slave.getSlaveState().equals(SlaveState.NOT_STARTED)) {
						slave.askForNode();
					}
				}				
			}
			
			ClientRequest req = null;
			try {
				req = this.inQ.poll(5000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// Dont Care
			}
			if (req != null) {
				handleClientRequest(req);			
			}
		}		
	}
	
	private void handleClientRequest(ClientRequest req) {
		if (req.scaleNum > 0) {
			for (int i = 0; i < req.scaleNum; i++) {
				slaves.add(new ContainerSlaveHandler(createContainerLaunchArgs(this.dArgs)));
			}
		}
	}

	private ContainerArgs createContainerLaunchArgs(DirtArgs ocArgs) {
		ContainerArgs containerArgs = new ContainerArgs();
//		containerArgs.appName = ocArgs.appName;
//		containerArgs.warLocation = ocArgs.warLocation;
		return containerArgs;
	}

	private AdminArgs createAdminLaunchArgs(DirtArgs ocArgs) {
		AdminArgs adminArgs = new AdminArgs();
//		elbArgs.maxPort = ocArgs.elbMaxPort;
//		elbArgs.minPort = ocArgs.elbMinPort;		
		return adminArgs;
	}

	public HeartbeatMaster getHeartbeatMaster() {
		return this.hbMaster;
	}
	
	public int getNumRunningContainers() {
		return this.numContainersRunning.get();		
	}
	
	public int getNumContainersDied() {
		return this.numContainersDied.get();		
	}
	
	public void kill() {
		for (SlaveHandler slave : slaves) {
			slave.kill();
		}
	}
	
	private HeartbeatMaster startHbMaster(ControllerClient client) throws InterruptedException {
		HBMasterArgs hbMasterArgs = new HBMasterArgs();
		hbMasterArgs.checkPeriod = this.dArgs.hbPeriod;
		hbMasterArgs.deadTime = this.dArgs.hbDeadTime;
		hbMasterArgs.warnTime = this.dArgs.hbWarnTime;
		hbMasterArgs.minPort = this.dArgs.hbMinPort;
		hbMasterArgs.maxPort = this.dArgs.hbMaxPort;
		hbMasterArgs.numCheckerThreads = this.dArgs.hbCheckerThreads;
		hbMasterArgs.numHbThreads = this.dArgs.hbMasterThreads;
		
		HeartbeatMaster hbMaster = new HeartbeatMaster(hbMasterArgs);
		hbMaster.init();
		hbMaster.registerClient(client.getHeartbeatClient());
		client.getExecutorService().submit(hbMaster);
		while (!hbMaster.isRunning()) {
			logger.debug("Waiting for HBMaster to start..");
			Thread.sleep(1000);
		}
		return hbMaster;
	}
	
	/**
	 * Placeholder class 
	 * @author suresa1
	 *
	 */
	public static class ClientRequest {
		
		public final int scaleNum;
		
		public ClientRequest(int scaleNum) {
			this.scaleNum = scaleNum;
		}
		
	}
	
	public static class ControllerRequest {
		
		public static enum ReqType {
			CONTAINER, LAUNCH;
		} 
		
		private final ReqType type;
		private volatile Node response;
		private volatile Node requestNode;  
		private LaunchContext launchContext;
		private SlaveHandler handler;
		
		public ControllerRequest(SlaveHandler handler) {
			this.type = ReqType.CONTAINER;
			this.handler = handler;
		}
		
		public ControllerRequest(LaunchContext lc, Node node) {
			this.type = ReqType.LAUNCH;
			this.launchContext = lc;
			this.requestNode = node;
		}		
		
		public ReqType getType() { return type; }
		
		public Node getRequestNode() {
			if (this.type.equals(ReqType.CONTAINER)) {
				return null;
			}
			return requestNode;
		}
		
		// Has to be called by AM
		public void setResponse(Node nodeResp) {
			this.response = response;
			this.handler.lease(nodeResp);
		}
		
		public LaunchContext getLaunchContext() {
			return launchContext;
		}
		
	}
	
	public static interface Node {
		public int getId();
	}
		
	public static enum SlaveState { 
		NOT_STARTED, NODE_REQUESTED, NODE_LEASED, NODE_LAUNCHED, NODE_RUNNING; 
	}	
	
	// Need to create a slaveHandler per container (proxy/webContainer).. and register with controller
	public abstract class SlaveHandler implements HeartbeatMasterClient {		
		
		protected volatile Node slaveNode = null;
		private final NodeType nType;
		private final Object launchArgs;
		
		private String sHost;
		private int sPort;
		
		protected volatile SlaveState state = SlaveState.NOT_STARTED;
		
		public SlaveHandler(NodeType nType, Object launchArgs) {
			this.nType = nType;
			this.launchArgs = launchArgs;
		}
		
		private void launchNode(Node preInitNode) {
			LaunchContext lContext = null;
			if (NodeType.CONTAINER.equals(nType)) {
				lContext = createContainerLaunchContext(preInitNode.getId(), (ContainerArgs)launchArgs);
			} else {
				lContext = createAdminLaunchContext(preInitNode.getId(), (AdminArgs)launchArgs);
			}
			ControllerRequest lRequest = new ControllerRequest(lContext, preInitNode);
			// Register for heartbeat for this node
			DirtController.this.hbMaster.registerClient(preInitNode.getId(), nType, this);
			DirtController.this.outQ.add(lRequest);
			this.state = SlaveState.NODE_LAUNCHED;
		}
		
		public String getRunningHost() {
			return this.sHost;
		}
		
		public void askForNode() {
			ControllerRequest req = new ControllerRequest(this);			
			DirtController.this.outQ.add(req);
			this.state = SlaveState.NODE_REQUESTED;
		}
		
		public void lease(Node node) {
			this.slaveNode = node;
			this.state = SlaveState.NODE_LEASED;
			launchNode(slaveNode);
		}
		
		public SlaveState getSlaveState() {
			return this.state;
		}
		
		public void kill() {
			TTransport transport = new TFramedTransport(new TSocket(sHost, sPort));
			try {
				transport.open();
				TBinaryProtocol protocol = new TBinaryProtocol(transport);
				THeartbeatCommandEndPoint.Client cl = new THeartbeatCommandEndPoint.Client(protocol);
				cl.killSelf();
			} catch (Exception e) {
				logger.error("Could not send Kill signal !!", e);
			}
		}
		
		/** Heartbeat Callback */		
		@Override
		public void nodeUp(HeartbeatNode node, NodeState nodeState) {
			if (nType.equals(NodeType.CONTAINER)) {
				DirtController.this.numContainersRunning.incrementAndGet();
			}
			this.sHost = nodeState.host;
			this.sPort = nodeState.port;
			this.state = SlaveState.NODE_RUNNING;
		}
		
		@Override
		public void nodeWarn(HeartbeatNode node, NodeState nodeState) {
			// TODO Auto-generated method stub			
		}
		
		@Override
		public void nodeDead(HeartbeatNode node, NodeState lastNodeState) {
			DirtController.this.numContainersDied.incrementAndGet();
			DirtController.this.numContainersRunning.decrementAndGet();
			this.slaveNode = null;
			this.state = SlaveState.NOT_STARTED;
		}
		
		private LaunchContext createAdminLaunchContext(int nodeId, AdminArgs adminArgs) {
			LinkedList<String> vargs = new LinkedList<String>();
			HashMap<String,String> env = new HashMap<String, String>();				
			
	        vargs.add("java");
//	        vargs.add("-cp");
//	        vargs.add("$CLASSPATH");

	        //vargs.add("-Xmx" + containerMemory + "m");
	 		//vargs.add("-Xdebug");
	 		//vargs.add("-Xrunjdwp:transport=dt_socket,address=8889,server=y,suspend=n");	        
	        vargs.add(DirtDaemon.class.getName());
	        
	        vargs.add("-admin");
	        vargs.add("true");
	        vargs.add("-min_port");
	        vargs.add("" + DirtController.this.dArgs.minPort);
	        vargs.add("-max_port");
	        vargs.add("" + DirtController.this.dArgs.maxPort);
	        vargs.add("-redis_port");
	        vargs.add("" + DirtController.this.dArgs.redisPort);
	        vargs.add("-redis_host");
	        vargs.add("" + DirtController.this.dArgs.redisHost);
	        addHBSlaveArgs(nodeId, vargs);
	        vargs.add("1>/tmp/dirt-admin-stdout" + nodeId);
	        vargs.add("2>/tmp/dirt-admin-stderr" + nodeId);
			
	        StringBuilder command = new StringBuilder();
	        for (CharSequence str : vargs) {
	            command.append(str).append(" ");
	        }		
			return new LaunchContext(command.toString(), env);
		}
				
		
		private LaunchContext createContainerLaunchContext(int nodeId, ContainerArgs containerArgs) {
			LinkedList<String> vargs = new LinkedList<String>();
			HashMap<String,String> env = new HashMap<String, String>();				
			
	        vargs.add("java");
//	        vargs.add("-cp");
//	        vargs.add("$CLASSPATH");

	        //vargs.add("-Xmx" + containerMemory + "m");
	 		//vargs.add("-Xdebug");
	 		//vargs.add("-Xrunjdwp:transport=dt_socket,address=8889,server=y,suspend=n");	        
	        vargs.add(DirtDaemon.class.getName());
	        
	        vargs.add("-container");
	        vargs.add("true");
	        vargs.add("-redis_port");
	        vargs.add("" + DirtController.this.dArgs.redisPort);
	        vargs.add("-redis_host");
	        vargs.add("" + DirtController.this.dArgs.redisHost);	        
	        addHBSlaveArgs(nodeId, vargs);
	        vargs.add("1>/tmp/dirt-container-stdout" + nodeId);
	        vargs.add("2>/tmp/dirt-container-stderr" + nodeId);
			
	        StringBuilder command = new StringBuilder();
	        for (CharSequence str : vargs) {
	            command.append(str).append(" ");
	        }		
			return new LaunchContext(command.toString(), env);		
		}
		
		private void addHBSlaveArgs(int nodeId, LinkedList<String> vargs) {
			vargs.add("-send_period");
			vargs.add("" + DirtController.this.dArgs.hbPeriod);
			vargs.add("-master_host");
			try {
				vargs.add(InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e) {
				logger.error("Exception while extracting hostname !!", e);
			}			
			vargs.add("-master_port");
			vargs.add("" + DirtController.this.hbMaster.getRunningPort());
			vargs.add("-send_timeout");
			vargs.add("" + (DirtController.this.dArgs.hbPeriod / 2));
			vargs.add("-node_id");
			vargs.add("" + nodeId);
			vargs.add("-num_sender_threads");
			vargs.add("1");
			vargs.add("-hb_min_port");
			vargs.add("" + DirtController.this.dArgs.hbMinPort);
			vargs.add("-hb_max_port");
			vargs.add("" + DirtController.this.dArgs.hbMaxPort);
		}
	}
	
	public class AdminSlaveHandler extends SlaveHandler {

		private int runningPort = -1;
		
		public AdminSlaveHandler(AdminArgs launchArgs) {
			super(NodeType.ADMIN, launchArgs);
		}
		
		@Override
		public void nodeUp(HeartbeatNode node, NodeState nodeState) {
			super.nodeUp(node, nodeState);
			this.runningPort = nodeState.nodeInfo.getAuxEndPointPort1();
		}
		
		public int getRunningPort() {
			return this.runningPort;
		}
	}	
	
	public class ContainerSlaveHandler extends SlaveHandler {

		private int runningPort = -1;
		
		public ContainerSlaveHandler(ContainerArgs launchArgs) {
			super(NodeType.CONTAINER, launchArgs);
		}
		
		@Override
		public void nodeUp(HeartbeatNode node, NodeState nodeState) {
			super.nodeUp(node, nodeState);
			this.runningPort = nodeState.nodeInfo.getAuxEndPointPort1();
		}
		
		public int getRunningPort() {
			return this.runningPort;
		}
	}	
	
}
