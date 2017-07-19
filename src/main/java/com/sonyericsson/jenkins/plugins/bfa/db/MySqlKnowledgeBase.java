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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.TypedQuery;

import org.hibernate.jpa.HibernatePersistenceProvider;
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

	private static final int MYSQL_DEFAULT_PORT = 3306;

	private final String host;
	private final int port;
	private final String dbName;
	private final String userName;
	private final Secret password;
	private final boolean enableStatistics;
	private final boolean successfulLogging;

	private transient EntityManagerFactory entityManagerFactory;

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
		final EntityManager manager = beginTransaction();
		final List<FailureCause> causes = manager.createQuery("select f from FailureCause f", FailureCause.class)
				.getResultList();
		for (final FailureCause f : causes) {
			loadLazyCollections(f);
		}
		endTransaction(manager);

		return causes;
	}

	@Override
	public Collection<FailureCause> getCauseNames() throws Exception {
		return getCauses();
	}

	@Override
	public Collection<FailureCause> getShallowCauses() throws Exception {
		return getCauses();
	}

	private EntityManager beginTransaction() {
		final EntityManager manager = entityManagerFactory.createEntityManager();
		manager.getTransaction().begin();
		return manager;
	}

	private void endTransaction(EntityManager manager) {
		manager.getTransaction().commit();
		manager.close();
	}

	@Override
	public FailureCause getCause(String id) throws Exception {
		final EntityManager manager = beginTransaction();
		final String sql = "select f from FailureCause f where f.id=:id";
		final TypedQuery<FailureCause> query = manager.createQuery(sql, FailureCause.class);
		query.setParameter("id", id);
		final List<FailureCause> causes = query.getResultList();
		if (causes.isEmpty()) {
			return null;
		}
		if (causes.size() > 1) {
			logger.log(Level.WARNING, "Multiple failure causes with id " + id);
		}
		final FailureCause cause = causes.get(0);
		loadLazyCollections(cause);
		endTransaction(manager);
		return cause;
	}

	private void loadLazyCollections(FailureCause f) {
		f.getModifications().size();
		f.getIndications().size();
	}

	@Override
	public FailureCause addCause(FailureCause cause) throws Exception {
		persist(cause);
		logger.info("Added failure cause '" + cause.getName() + "' with id " + cause.getId());
		return getCause(cause.getId());
	}

	@Override
	public FailureCause removeCause(String id) throws Exception {
		final FailureCause cause = getCause(id);
		if (cause == null) {
			logger.log(Level.WARNING,
					"Cannot remove failure cause with id " + id);
			return null;
		}
		final EntityManager manager = beginTransaction();
		manager.remove(manager.contains(cause) ? cause : manager.merge(cause));
		endTransaction(manager);
		logger.info("Removed failure cause '" + cause.getName() + "' with id " + cause.getId());
		return cause;
	}

	@Override
	public FailureCause saveCause(FailureCause cause) throws Exception {
		if (getCause(cause.getId()) == null) {
			logger.log(Level.WARNING, "Failure cause with id " + cause.getId()
					+ " not available in database. Persisting it.");
			return addCause(cause);
		}
		final EntityManager manager = beginTransaction();
		final FailureCause merged = manager.merge(cause);
		endTransaction(manager);
		logger.info("Updated failure cause '" + merged.getName() + "' with id " + merged.getId());
		return merged;
	}

	@Override
	public void convertFrom(KnowledgeBase oldKnowledgeBase) throws Exception {
		if (!equals(oldKnowledgeBase)) {
			if (oldKnowledgeBase instanceof MongoDBKnowledgeBase) {
				convertFromAbstract(oldKnowledgeBase);
			} else {
				final Collection<FailureCause> fcs = oldKnowledgeBase.getCauseNames();
				logger.info("Converting " + fcs.size() + " FailureCauses to SQLKnowledge base.");
				for (final FailureCause cause : fcs) {
					// try finding the id in the knowledgebase, if so, update it.
					if (getCause(cause.getId()) != null) {
						saveCause(cause);
						// if not found, add a new.
					} else {
						cause.setId(null);
						addCause(cause);
					}
				}
			}
		}
	}

	@Override
	public List<String> getCategories() throws Exception {
		final List<String> categories = new LinkedList<String>();
		final Collection<FailureCause> causes = getCauses();
		for (final FailureCause cause : causes) {
			for (final String category : cause.getCategories()) {
				if (!categories.contains(category)) {
					categories.add(category);
				}
			}
		}
		return categories;
	}

	@Override
	public boolean equals(KnowledgeBase oldKnowledgeBase) {
		if (this == oldKnowledgeBase) {
			return true;
		}
		if (getClass().isInstance(oldKnowledgeBase)) {
			final MySqlKnowledgeBase other = (MySqlKnowledgeBase) oldKnowledgeBase;
			if (dbName == null) {
				if (other.dbName != null) {
					return false;
				}
			} else if (!dbName.equals(other.dbName)) {
				return false;
			}
			if (enableStatistics != other.enableStatistics) {
				return false;
			}
			if (host == null) {
				if (other.host != null) {
					return false;
				}
			} else if (!host.equals(other.host)) {
				return false;
			}
			if (password == null) {
				if (other.password != null) {
					return false;
				}
			} else if (!password.equals(other.password)) {
				return false;
			}
			if (port != other.port) {
				return false;
			}
			if (successfulLogging != other.successfulLogging) {
				return false;
			}
			if (userName == null) {
				if (other.userName != null) {
					return false;
				}
			} else if (!userName.equals(other.userName)) {
				return false;
			}
			return true;
		}
		return false;
	}

	@Override
	public void start() throws Exception {
		try {
			String url = this.host.startsWith("jdbc:mysql://")
					? this.host
					: "jdbc:mysql://" + this.host;
			url = url + ":" + this.port + "/" + this.dbName;

			// Additional properties for persistence.xml
			final Properties eProps = new Properties();
			eProps.setProperty("javax.persistence.jdbc.url", url);
			eProps.setProperty("javax.persistence.jdbc.user", this.userName);
			eProps.setProperty("javax.persistence.jdbc.password",
					Secret.toString(this.password));

			// provider can't be found with Persistence.createEntityManagerFactory because of
			// packing. use hibernate directly.
			entityManagerFactory = new HibernatePersistenceProvider().createEntityManagerFactory("bfa",
					eProps);
		} catch (final Throwable ex) {
			throw new ExceptionInInitializerError(ex);
		}
	}

	@Override
	public void stop() {
		if (entityManagerFactory != null && entityManagerFactory.isOpen()) {
			entityManagerFactory.close();
		}
	}

	@Override
	public void saveStatistics(Statistics stat) throws Exception {
		persist(stat);
	}

	private void persist(Object entity) {
		final EntityManager manager = beginTransaction();
		manager.persist(entity);
		endTransaction(manager);
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
		 * @return {@link hudson.util.FormValidation#ok()} if everything is well.
		 */
		public FormValidation doCheckHost(
				@QueryParameter("value") final String value) {
			if (Util.fixEmpty(value) == null) {
				return FormValidation.error("Please provide a host name!");
			} else {
				final Matcher m = Pattern.compile("\\s").matcher(value);
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
		 * @return {@link hudson.util.FormValidation#ok()} if everything is well.
		 */
		public FormValidation doCheckPort(
				@QueryParameter("value") String value) {
			try {
				Long.parseLong(value);
				return FormValidation.ok();
			} catch (final NumberFormatException e) {
				return FormValidation.error("Please provide a port number!");
			}
		}

		/**
		 * Checks that the database name is not empty.
		 *
		 * @param value
		 *            the database name to check.
		 * @return {@link hudson.util.FormValidation#ok()} if everything is well.
		 */
		public FormValidation doCheckDBName(
				@QueryParameter("value") String value) {
			if (value == null || value.isEmpty()) {
				return FormValidation.error("Please provide a database name!");
			} else {
				final Matcher m = Pattern.compile("\\s").matcher(value);
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
			} catch (final ClassNotFoundException e) {
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
			} catch (final SQLException e) {
				return FormValidation.error(e,
						Messages.MySqlKnowledgeBase_ConnectionError());
			}
		}
	}
}
