/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.xd.dirt.container.DefaultContainer;
import org.springframework.xd.dirt.listener.util.BannerUtils;
import org.springframework.xd.dirt.stream.StreamServer;

/**
 * The main driver class for the admin.
 *
 * @author Mark Pollack
 * @author Jennifer Hickey
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 * @author Eric Bottard
 */
public class AdminMain {

	private static final Log logger = LogFactory.getLog(AdminMain.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AdminOptions options = new AdminOptions();
		CmdLineParser parser = new CmdLineParser(options);
		try {
			parser.parseArgument(args);
		}
		catch (CmdLineException e) {
			logger.error(e.getMessage());
			parser.printUsage(System.err);
			System.exit(1);
		}
		
		setSystemProps(options);
		
		if (options.isShowHelp()) {
			parser.printUsage(System.err);
			System.exit(0);
		}
		launchStreamServer(options);
	}
	 
	public static void setSystemProps(AbstractOptions options) {
		AbstractOptions.setXDHome(options.getXDHomeDir());
		AbstractOptions.setXDTransport(options.getTransport());
		AbstractOptions.setSystemRedisPort(options.getRedisPort());
		AbstractOptions.setSystemRedisHost(options.getRedisHost());		
	}

	/**
	 * Launch stream server with the given home and transport
	 */
	public static void launchStreamServer(final AdminOptions options) {
		try {
			XmlWebApplicationContext context = new XmlWebApplicationContext();
			context.setConfigLocation("classpath:"
					+ DefaultContainer.XD_INTERNAL_CONFIG_ROOT + "admin-server.xml");
			context.refresh();

			// Not making StreamServer a spring bean eases move to .war file if needed
			final StreamServer server = new StreamServer(context, options.getHttpPort());
			server.afterPropertiesSet();
			server.start();
			if (Transport.local == options.getTransport()) {
				System.out.println(BannerUtils.displayBanner(null,
					String.format("Running in Local Mode on Port %s ", server.getPort())));
			}
			context.addApplicationListener(new ApplicationListener<ContextClosedEvent>() {
				@Override
				public void onApplicationEvent(ContextClosedEvent event) {
					server.stop();
				}
			});
			context.registerShutdownHook();
		}
		catch (RedisConnectionFailureException e) {
			final Log logger = LogFactory.getLog(StreamServer.class);
			logger.fatal(e.getMessage());
			System.err.println("Redis does not seem to be running. Did you install and start Redis? "
					+ "Please see the Getting Started section of the guide for instructions.");
			System.exit(1);
		}
	}

}
