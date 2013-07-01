package org.springframework.xd.dirt.yarn.controller;

import java.util.Map;

public class LaunchContext {
	
	private final String shellCommand;	
	private final Map<String, String> env;

	public LaunchContext(String shellCommand, Map<String, String> env) {
		this.shellCommand = shellCommand;
		this.env = env;
	}

	public String getShellCommand() {
		return shellCommand;
	}

	public Map<String, String> getEnv() {
		return env;
	}
}

