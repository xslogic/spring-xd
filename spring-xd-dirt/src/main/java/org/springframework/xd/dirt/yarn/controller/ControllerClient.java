package org.springframework.xd.dirt.yarn.controller;

import java.util.concurrent.ExecutorService;

import org.springframework.xd.dirt.yarn.hmon.HeartbeatMasterClient;

public interface ControllerClient {
	
	public HeartbeatMasterClient getHeartbeatClient();	
	
	public ExecutorService getExecutorService();

}
