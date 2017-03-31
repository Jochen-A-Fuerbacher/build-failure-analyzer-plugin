/*
 * The MIT License
 *
 * Copyright 2017 Jochen A. Fuerbacher, 1&1 Telecommunication SE. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.sonyericsson.jenkins.plugins.bfa.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import com.sonyericsson.jenkins.plugins.bfa.Messages;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;
import com.sonyericsson.jenkins.plugins.bfa.statistics.Statistics;

import hudson.Extension;
import hudson.Util;
import hudson.model.Descriptor;
import hudson.util.FormValidation;
import hudson.util.Secret;
import jenkins.model.Jenkins;

public class MySqlKnowledgeBase extends KnowledgeBase {

	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

	private static final Logger logger = Logger
			.getLogger(MySqlKnowledgeBase.class.getName());

	private static SessionFactory factory;

	private static final int MYSQL_DEFAULT_PORT = 3306;

	private String host;
	private int port;
	private String dbName;
	private String userName;
	private Secret password;
	private boolean enableStatistics;
	private boolean successfulLogging;

	private EntityManagerFactory entityManagerFactory;

	/**
	 * Getter for the SQL DB user name.
	 * 
	 * @return the user name.
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * Getter for the SQL DB password.
	 * 
	 * @return the password.
	 */
	public Secret getPassword() {
		return password;
	}

	/**
	 * Getter for the host value.
	 * 
	 * @return the host string.
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Getter for the port value.
	 * 
	 * @return the port number.
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Getter for the database name value.
	 * 
	 * @return the database name string.
	 */
	public String getDbName() {
		return dbName;
	}

	@Override
	public boolean isStatisticsEnabled() {
		return enableStatistics;
	}

	@Override
	public boolean isSuccessfulLoggingEnabled() {
		return successfulLogging;
	}

	/**
	 * Standard constructor.
	 * 
	 * @param host
	 *            the host to connect to.
	 * @param port
	 *            the port to connect to.
	 * @param dbName
	 *            the database name to connect to.
	 * @param userName
	 *            the user name for the database.
	 * @param password
	 *            the password for the database.
	 * @param enableStatistics
	 *            if statistics logging should be enabled or not.
	 * @param successfulLogging
	 *            if all builds should be logged to the statistics DB
	 */
	@DataBoundConstructor
	public MySqlKnowledgeBase(String host, int port, String dbName,
			String userName, Secret password, boolean enableStatistics,
			boolean successfulLogging) {
		this.host = host;
		this.port = port;
		this.dbName = dbName;
		this.userName = userName;
		this.password = password;
		this.enableStatistics = enableStatistics;
		this.successfulLogging = successfulLogging;
	}

	@Override
	public Descriptor<KnowledgeBase> getDescriptor() {
		return Jenkins.getInstance()
				.getDescriptorByType(MySqlKnowledgeBaseDescriptor.class);
	}

	@Override
	public Collection<FailureCause> getCauses() throws Exception {

		EntityManager manager = entityManagerFactory.createEntityManager();
		manager.getTransaction().begin();
		List<FailureCause> causes = manager.createQuery("from FAILURECAUSE")
				.getResultList();
		manager.getTransaction().commit();
		manager.close();

		return causes;
	}

	@Override
	public Collection<FailureCause> getCauseNames() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<FailureCause> getShallowCauses() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FailureCause getCause(String id) throws Exception {
		EntityManager manager = entityManagerFactory.createEntityManager();
		manager.getTransaction().begin();
		List<FailureCause> causes = manager
				.createQuery("from FAILURECAUSE where id=" + id)
				.getResultList();
		if (causes.size() != 1) {
			logger.log(Level.WARNING, "Multiple failure causes with id " + id);
			return null;
		}
		FailureCause cause = causes.get(0);

		manager.getTransaction().commit();
		manager.close();
		return cause;
	}

	@Override
	public FailureCause addCause(FailureCause cause) throws Exception {
		String id = cause.getId();
		EntityManager manager = entityManagerFactory.createEntityManager();
		manager.getTransaction().begin();
		manager.persist(cause);
		manager.getTransaction().commit();
		manager.close();

		return getCause(id);
	}

	@Override
	public FailureCause removeCause(String id) throws Exception {
		FailureCause cause = getCause(id);
		if (cause == null) {
			logger.log(Level.WARNING,
					"Cannot remove failure cause with id " + id);
			return null;
		}
		EntityManager manager = entityManagerFactory.createEntityManager();
		manager.getTransaction().begin();
		manager.remove(cause);
		manager.getTransaction().commit();
		manager.close();

		return cause;
	}

	@Override
	public FailureCause saveCause(FailureCause cause) throws Exception {
		EntityManager manager = entityManagerFactory.createEntityManager();
		manager.getTransaction().begin();
		if (!manager.contains(cause)) {
			logger.log(Level.WARNING,
					"Cannot save failure cause with id " + cause.getId()
							+ ": \n"
							+ "Failure cause not available in database.");
			return cause;
		}

		return null;
	}

	@Override
	public void convertFrom(KnowledgeBase oldKnowledgeBase) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> getCategories() throws Exception {
		List<String> categories = new LinkedList<String>();

		Collection<FailureCause> causes = getCauses();
		for (FailureCause cause : causes) {
			for (String category : cause.getCategories()) {
				if (!categories.contains(category)) {
					categories.add(category);
				}
			}
		}

		return categories;
	}

	@Override
	public boolean equals(KnowledgeBase oldKnowledgeBase) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void start() throws Exception {
		try {
			String url = this.host.startsWith("jdbc:mysql://")
					? this.host
					: "jdbc:mysql://" + this.host;
			url = url + ":" + this.port + "/" + this.dbName;

			Properties prop = new Properties();
			prop.setProperty("hibernate.connection.driver_class", MYSQL_DRIVER);
			prop.setProperty("hibernate.connection.url", url);
			prop.setProperty("hibernate.connection.username", this.userName);
			prop.setProperty("hibernate.connection.password",
					Secret.toString(this.password));
			prop.setProperty("dialect", "org.hibernate.dialect.MySQLDialect");

			factory = new Configuration()
					.addProperties(prop)
					.buildSessionFactory();
			
			Properties eProps = new Properties();
			eProps.setProperty("javax.persistence.jdbc.url", url);
			eProps.setProperty("javax.persistence.jdbc.user", this.userName);
			eProps.setProperty("javax.persistence.jdbc.password", Secret.toString(this.password));

			entityManagerFactory = Persistence
					.createEntityManagerFactory("bfa", eProps);
		} catch (Throwable ex) {
			throw new ExceptionInInitializerError(ex);
		}
	}

	@Override
	public void stop() {
		entityManagerFactory.close();
		factory.close();
	}

	@Override
	public void saveStatistics(Statistics stat) throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * Descriptor for {@link MySqlKnowledgeBase}.
	 */
	@Extension
	public static class MySqlKnowledgeBaseDescriptor
			extends
				KnowledgeBaseDescriptor {

		@Override
		public String getDisplayName() {
			return Messages.MySqlKnowledgeBase_DisplayName();
		}

		/**
		 * Convenience method for jelly.
		 * 
		 * @return the default port.
		 */
		public int getDefaultPort() {
			return MYSQL_DEFAULT_PORT;
		}

		/**
		 * Checks that the host name is not empty.
		 *
		 * @param value
		 *            the pattern to check.
		 * @return {@link hudson.util.FormValidation#ok()} if everything is
		 *         well.
		 */
		public FormValidation doCheckHost(
				@QueryParameter("value") final String value) {
			if (Util.fixEmpty(value) == null) {
				return FormValidation.error("Please provide a host name!");
			} else {
				Matcher m = Pattern.compile("\\s").matcher(value);
				if (m.find()) {
					return FormValidation
							.error("Host name contains white space!");
				}
				return FormValidation.ok();
			}
		}

		/**
		 * Checks that the port number is not empty and is a number.
		 *
		 * @param value
		 *            the port number to check.
		 * @return {@link hudson.util.FormValidation#ok()} if everything is
		 *         well.
		 */
		public FormValidation doCheckPort(
				@QueryParameter("value") String value) {
			try {
				Long.parseLong(value);
				return FormValidation.ok();
			} catch (NumberFormatException e) {
				return FormValidation.error("Please provide a port number!");
			}
		}

		/**
		 * Checks that the database name is not empty.
		 *
		 * @param value
		 *            the database name to check.
		 * @return {@link hudson.util.FormValidation#ok()} if everything is
		 *         well.
		 */
		public FormValidation doCheckDBName(
				@QueryParameter("value") String value) {
			if (value == null || value.isEmpty()) {
				return FormValidation.error("Please provide a database name!");
			} else {
				Matcher m = Pattern.compile("\\s").matcher(value);
				if (m.find()) {
					return FormValidation
							.error("Database name contains white space!");
				}
				return FormValidation.ok();
			}
		}
		/**
		 * Tests if the provided parameters can connect to the Mongo database.
		 * 
		 * @param host
		 *            the host name.
		 * @param port
		 *            the port.
		 * @param dbName
		 *            the database name.
		 * @param userName
		 *            the user name.
		 * @param password
		 *            the password.
		 * @return {@link FormValidation#ok() } if can be done,
		 *         {@link FormValidation#error(java.lang.String) } otherwise.
		 * @throws ClassNotFoundException
		 */
		public FormValidation doTestConnection(
				@QueryParameter("host") final String host,
				@QueryParameter("port") final int port,
				@QueryParameter("dbName") final String dbName,
				@QueryParameter("userName") final String userName,
				@QueryParameter("password") final String password) {

			try {
				Class.forName(MYSQL_DRIVER);
			} catch (ClassNotFoundException e) {
				return FormValidation.error(e,
						Messages.MySqlKnowledgeBase_ConnectionError());
			}

			try {
				String url = host.startsWith("jdbc:mysql://")
						? host
						: "jdbc:mysql://" + host;
				url = url + ":" + port + "/" + dbName;
				Connection conn = null;
				conn = DriverManager.getConnection(url, userName, password);
				conn.close();
				return FormValidation
						.ok(Messages.MySqlKnowledgeBase_ConnectionOK());
			} catch (SQLException e) {
				return FormValidation.error(e,
						Messages.MySqlKnowledgeBase_ConnectionError());
			}
		}
	}
}
