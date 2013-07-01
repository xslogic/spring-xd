package org.springframework.xd.dirt.yarn;

import com.beust.jcommander.Parameter;

public class DirtArgs {

	@Parameter(names = "-container_mem", description = "YARN parameter: Container memory", required = false)
	public int containerMemory = 256;
	
	@Parameter(names = "-priority", description = "Priority ??", required = false)
	public int priority = 0;
	
	@Parameter(names = "-user", description = "User name", required = false)
	public String user = "";	
	
	@Parameter(names = "-num_containers", description = "YARN parameter: Number of containers on which the Dirt needs to be hosted", required = true)
	public int numContainers = 3;

	@Parameter(names = "-hb_period", description = "Heartbeat period", required = false)
	public int hbPeriod = 10000;
	
	@Parameter(names = "-hb_warn_time", description = "Time since last heartbeat for issuing a warning to client", required = false)
	public int hbWarnTime = 15000;
	
	@Parameter(names = "-hb_dead_time", description = "Time since last heartbeat for issuing a node dead msg to client", required = false)
	public int hbDeadTime = 30000;	
	
	@Parameter(names = "-hb_num_check_threads", description = "Number of threads allocated to checking the status of all nodes", required = false)
	public int hbCheckerThreads = 1;
	
	@Parameter(names = "-hb_num_master_threads", description = "Number of threads for the Heartbeat master", required = false)
	public int hbMasterThreads = 2;
	
	@Parameter(names = "-hb_min_port", description = "Min port on which HB master would listen on", required = false)
	public int hbMinPort = 8100;
	
	@Parameter(names = "-hb_max_port", description = "Miax port on which HB master would listen on", required = false)
	public int hbMaxPort = 8200;
	
	@Parameter(names = "-min_port", description = "Min port on which the XD Admin and Container would listen on", required = false)
	public int minPort = 8100;
	
	@Parameter(names = "-max_port", description = "Max port on which the XD Admin and Container would listen on", required = false)
	public int maxPort = 8200;	
	
	@Parameter(names = "-redis_port", description = "Redis Port", required = true)
	public int redisPort = 6379;
	
	@Parameter(names = "-redis_host", description = "Redis Host", required = true)
	public String redisHost = "localhost";	
	
	@Parameter(names = "-xd_dir", description = "XD installation directory", required = true)
	public String xdDir;
	
	@Parameter(names = "-lib_files", description = "files in lib dir", required = false)
	public String libFiles;
	
	@Parameter(names = "-jvm_debug", description = "Enable JVM debug", required = false)
	public boolean debug = false;	
}
