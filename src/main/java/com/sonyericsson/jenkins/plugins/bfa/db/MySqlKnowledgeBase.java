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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Tuple;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.ListJoin;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.collections.keyvalue.MultiKey;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.jfree.data.time.Day;
import org.jfree.data.time.Hour;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimePeriod;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import com.sonyericsson.jenkins.plugins.bfa.Messages;
import com.sonyericsson.jenkins.plugins.bfa.graphs.FailureCauseTimeInterval;
import com.sonyericsson.jenkins.plugins.bfa.graphs.GraphFilterBuilder;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCauseModification;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCauseModification_;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause_;
import com.sonyericsson.jenkins.plugins.bfa.model.indication.BuildLogIndication;
import com.sonyericsson.jenkins.plugins.bfa.model.indication.Indication;
import com.sonyericsson.jenkins.plugins.bfa.model.indication.MultilineBuildLogIndication;
import com.sonyericsson.jenkins.plugins.bfa.statistics.FailureCauseStatistics;
import com.sonyericsson.jenkins.plugins.bfa.statistics.FailureCauseStatistics_;
import com.sonyericsson.jenkins.plugins.bfa.statistics.Statistics;
import com.sonyericsson.jenkins.plugins.bfa.statistics.Statistics_;
import com.sonyericsson.jenkins.plugins.bfa.utils.ObjectCountPair;

import hudson.Extension;
import hudson.Util;
import hudson.model.Descriptor;
import hudson.util.FormValidation;
import hudson.util.Secret;
import jenkins.model.Jenkins;

public class MySqlKnowledgeBase extends CachedKnowledgeBase {

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
	protected DBKnowledgeBaseCache buildCache() throws Exception {
		return new DBKnowledgeBaseCache() {
			@Override
			protected List<FailureCause> updateCausesList() {
				return MySqlKnowledgeBase.this.getCauses(true);
			}

			@Override
			protected List<String> updateCategories() {
				final EntityManager manager = beginTransaction();
				final CriteriaBuilder c = manager.getCriteriaBuilder();
				final CriteriaQuery<String> cquery = c.createQuery(String.class);
				final Root<FailureCause> f = cquery.from(FailureCause.class);
				final TypedQuery<String> query = manager
						.createQuery(cquery.select(f.join(FailureCause_.categories))
								.where(c.isFalse(f.get(FailureCause_.deleted))).distinct(true));
				final List<String> result = query.getResultList();
				endTransaction(manager);
				return result;
			}
		};
	}

	@Override
	public Date getLatestFailureForCause(String id) {
		final EntityManager manager = beginTransaction();
		final CriteriaBuilder c = manager.getCriteriaBuilder();
		final CriteriaQuery<Date> q = c.createQuery(Date.class);
		final Root<Statistics> s = q.from(Statistics.class);
		final ListJoin<Statistics, FailureCauseStatistics> join = s.join(Statistics_.failureCauseStatisticsList);
		final Path<String> fcId = join.get(FailureCauseStatistics_.id);
		final Path<Date> startingTime = s.get(Statistics_.startingTime);
		final TypedQuery<Date> query = manager.createQuery(q.select(c.greatest(startingTime)).where(c.equal(fcId, id)));
		final Date latest = query.getSingleResult();
		endTransaction(manager);
		return latest;
	}

	@Override
	public void updateLastSeen(List<String> ids, Date seen) {
		try {
			for (final String id : ids) {
				final FailureCause f = getCause(id);
				f.setLastOccurred(seen);
				saveCause(f);
			}
		} catch (final Exception e) {
			logger.log(Level.SEVERE, "Updating last seen date failed", e);
		}
	}

	@Override
	public Date getCreationDateForCause(String id) {
		final EntityManager manager = beginTransaction();
		final CriteriaBuilder c = manager.getCriteriaBuilder();
		final CriteriaQuery<Date> q = c.createQuery(Date.class);
		final Root<FailureCause> fc = q.from(FailureCause.class);
		final Path<Date> time = fc.join(FailureCause_.modifications).get(FailureCauseModification_.time);
		final TypedQuery<Date> query = manager.createQuery(q.select(c.least(time))
				.where(c.equal(fc.get(FailureCause_.id), id)));
		final Date created = query.getSingleResult();
		endTransaction(manager);
		return created;
	}

	@Override
	public Descriptor<KnowledgeBase> getDescriptor() {
		return Jenkins.getInstance()
				.getDescriptorByType(MySqlKnowledgeBaseDescriptor.class);
	}

	private List<FailureCause> getCauses(boolean loadLazyCollections) {
		final EntityManager manager = beginTransaction();
		CriteriaBuilder cb = manager.getCriteriaBuilder();
		final CriteriaQuery<FailureCause> query = cb.createQuery(FailureCause.class);

		final Root<FailureCause> r = query.from(FailureCause.class);
		final List<FailureCause> causes = manager
				.createQuery(query.select(r).where(cb.isFalse(r.get(FailureCause_.deleted)))).getResultList();
		final List<FailureCause> result;
		if (loadLazyCollections) {
			for (final FailureCause f : causes) {
				loadLazyCollections(f);
			}
			result = causes;
		} else {
			result = new LinkedList<FailureCause>();
			for (final FailureCause f : causes) {
				// replace not loaded collections with empty ones
				result.add(new FailureCause(f.getId(), f.getName(), f.getDescription(), f.getComment(),
						f.getLastOccurred(), f.getCategories(), new LinkedList<Indication>(),
						new LinkedList<FailureCauseModification>()));
			}
		}
		endTransaction(manager);
		return result;
	}

	@Override
	public Collection<FailureCause> getCauseNames() throws Exception {
		return getCauses(false);
	}

	@Override
	public Collection<FailureCause> getShallowCauses() throws Exception {
		// needs to be true because modifications are fetched after transaction ends.
		return getCauses(true);
	}

	private synchronized EntityManager beginTransaction() {
		final EntityManager manager = entityManagerFactory.createEntityManager();
		manager.getTransaction().begin();
		return manager;
	}

	private synchronized void endTransaction(EntityManager manager) {
		manager.getTransaction().commit();
		manager.close();
	}

	@Override
	public FailureCause getCause(String id) {
		final EntityManager manager = beginTransaction();
		final CriteriaBuilder c = manager.getCriteriaBuilder();
		final CriteriaQuery<FailureCause> cquery = c.createQuery(FailureCause.class);
		final Root<FailureCause> f = cquery.from(FailureCause.class);

		final TypedQuery<FailureCause> query = manager.createQuery(cquery.where(c.equal(f.get(FailureCause_.id), id)));
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

	/**
	 * try to access modifications and indications so that hibernate fetches them from db, because
	 * it doesn't allow for fetching multiple bags in one query.
	 *
	 * @param f
	 */
	private void loadLazyCollections(FailureCause f) {
		f.getModifications().size();
		f.getIndications().size();
	}

	@Override
	protected FailureCause persistCause(FailureCause cause) throws Exception {
		persist(cause);
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
		cause.setDeleted(true);
		mergeCause(cause);
		logger.info("Removed failure cause '" + cause.getName() + "' with id " + cause.getId());
		updateCache();
		return cause;
	}

	@Override
	protected FailureCause mergeCause(FailureCause cause) throws Exception {
		if (getCause(cause.getId()) == null) {
			logger.log(Level.WARNING, "Failure cause with id " + cause.getId()
					+ " not available in database. Persisting it.");
			return addCause(cause, true);
		}
		final EntityManager manager = beginTransaction();
		final FailureCause merged = manager.merge(cause);
		endTransaction(manager);
		return merged;
	}

	@Override
	public void convertFrom(KnowledgeBase oldKnowledgeBase) throws Exception {
		if (!equals(oldKnowledgeBase)) {
			final Collection<FailureCause> fcs = oldKnowledgeBase.getCauses();
			logger.info("Converting " + fcs.size() + " FailureCauses to MySqlKnowledgeBase.");
			for (final FailureCause cause : fcs) {
				// try finding the id in the knowledgebase, if so, update it.
				if (getCause(cause.getId()) != null) {
					saveCause(cause, false);
				} else {
					// if not found, add a new.
					cause.setId(null);
					// unset ids for hibernate
					final List<FailureCauseModification> mods = new ArrayList<FailureCauseModification>();
					for (final FailureCauseModification m : cause.getModifications()) {
						mods.add(new FailureCauseModification(m.getUser(), m.getTime()));
					}
					cause.getModifications().clear();
					cause.getModifications().addAll(mods);
					final List<Indication> inds = new ArrayList<Indication>();
					for (final Indication i : cause.getIndications()) {
						Indication n;
						// new types of indications need to be added here.
						if (i instanceof MultilineBuildLogIndication) {
							n = new MultilineBuildLogIndication(i.getUserProvidedExpression());
						} else {
							n = new BuildLogIndication(i.getUserProvidedExpression());
						}
						inds.add(n);
					}
					cause.getIndications().clear();
					cause.getIndications().addAll(inds);
					addCause(cause, false);
				}
			}
			updateCache();
		}
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
	public void start() {
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
			entityManagerFactory = new HibernatePersistenceProvider()
					.createEntityManagerFactory("bfa", eProps);
			super.start();
		} catch (final Throwable ex) {
			throw new ExceptionInInitializerError(ex);
		}
	}

	@Override
	public void stop() {
		super.stop();
		if (entityManagerFactory != null && entityManagerFactory.isOpen()) {
			entityManagerFactory.close();
		}
	}

	private <T> CriteriaQuery<T> getQuery(CriteriaQuery<T> q, EntityManager m, GraphFilterBuilder filter) {
		return getQuery(q, q.from(Statistics.class), m, filter);
	}

	private <T> CriteriaQuery<T> getQuery(CriteriaQuery<T> q, Root<Statistics> r, EntityManager m,
			GraphFilterBuilder filter) {
		final CriteriaBuilder c = m.getCriteriaBuilder();
		final List<Predicate> restrictions = new ArrayList<Predicate>(7);
		addEqualFilter(c, r, restrictions, Statistics_.master, filter.getMasterName());
		addEqualFilter(c, r, restrictions, Statistics_.slave, filter.getSlaveName());
		addEqualFilter(c, r, restrictions, Statistics_.projectName, filter.getProjectName());
		addEqualFilter(c, r, restrictions, Statistics_.result, filter.getResult());
		if (filter.getBuildNumbers() != null && !filter.getBuildNumbers().isEmpty()) {
			restrictions.add(r.get(Statistics_.buildNumber).in(filter.getBuildNumbers()));
		}
		if (filter.getExcludeResult() != null) {
			restrictions.add(c.notEqual(r.get(Statistics_.result), filter.getExcludeResult()));
		}
		if (filter.getSince() != null) {
			restrictions.add(c.greaterThanOrEqualTo(r.get(Statistics_.startingTime), filter.getSince()));
		}
		final Predicate[] array = restrictions.toArray(new Predicate[restrictions.size()]);
		return q.where(c.and(array));
	}

	private <T> void addEqualFilter(CriteriaBuilder c, Root<Statistics> r, Collection<Predicate> restrictions,
			SingularAttribute<Statistics, T> attribute, T criteria) {
		if (criteria != null) {
			restrictions.add(c.equal(r.get(attribute), criteria));
		}
	}

	/**
	 * @param limit
	 * @param tq
	 */
	private void addLimit(int limit, final TypedQuery<?> tq) {
		if (limit > 0) {
			tq.setMaxResults(limit);
		}
	}

	@Override
	public List<Statistics> getStatistics(GraphFilterBuilder filter, int limit) {
		final EntityManager manager = beginTransaction();
		final TypedQuery<Statistics> tq = getStatisticsQuery(manager, filter);
		addLimit(limit, tq);
		final List<Statistics> result = tq.getResultList();
		endTransaction(manager);
		return result;
	}

	private TypedQuery<Statistics> getStatisticsQuery(EntityManager manager, GraphFilterBuilder filter) {
		final CriteriaQuery<Statistics> q = manager.getCriteriaBuilder().createQuery(Statistics.class);
		final CriteriaQuery<Statistics> query = getQuery(q, manager, filter);
		return manager.createQuery(query);
	}

	@Override
	public List<ObjectCountPair<FailureCause>> getNbrOfFailureCauses(GraphFilterBuilder filter) {
		return getNbrOfFailureCauses(filter, 0);
	}

	public List<ObjectCountPair<FailureCause>> getNbrOfFailureCauses(GraphFilterBuilder filter, int limit) {
		final List<ObjectCountPair<String>> fcsPerId = getNbrOfFailureCausesPerId(filter, limit);

		// get FailureCauses corresponding to found ids
		final Collection<String> ids = new ArrayList<String>(fcsPerId.size());
		for (final ObjectCountPair<String> t : fcsPerId) {
			ids.add(t.getObject());
		}

		final Map<String, FailureCause> fcs = getFailureCauses(ids);

		// build result list with FailureCauses and counts
		final List<ObjectCountPair<FailureCause>> list = new ArrayList<ObjectCountPair<FailureCause>>(fcsPerId.size());
		for (final ObjectCountPair<String> t : fcsPerId) {
			FailureCause fc = fcs.get(t.getObject());
			if (fc != null) {
				list.add(new ObjectCountPair<FailureCause>(fc, t.getCount()));
			}
		}
		return list;
	}

	/**
	 * @param ids
	 * @return
	 */
	private Map<String, FailureCause> getFailureCauses(final Collection<String> ids) {
		final Map<String, FailureCause> fcs = new HashMap<String, FailureCause>();
		if (!ids.isEmpty()) {
			final EntityManager manager = beginTransaction();
			final CriteriaBuilder c = manager.getCriteriaBuilder();
			final CriteriaQuery<FailureCause> fq = c.createQuery(FailureCause.class);
			final Root<FailureCause> rf = fq.from(FailureCause.class);
			final TypedQuery<FailureCause> query = manager.createQuery(fq.where(rf.get(FailureCause_.id).in(ids)));
			for (final FailureCause f : query.getResultList()) {
				fcs.put(f.getId(), f);
			}
			endTransaction(manager);
		}
		return fcs;
	}

	@Override
	public List<ObjectCountPair<String>> getNbrOfFailureCausesPerId(GraphFilterBuilder filter, int limit) {
		final EntityManager manager = beginTransaction();
		final CriteriaBuilder c = manager.getCriteriaBuilder();
		final CriteriaQuery<Tuple> cq = c.createTupleQuery();
		final Root<Statistics> s = cq.from(Statistics.class);
		CriteriaQuery<Tuple> q = getQuery(cq, s, manager, filter);
		// build the selections
		final Expression<String> fc = s.join(Statistics_.failureCauseStatisticsList).get(FailureCauseStatistics_.id);
		final Expression<Long> count = c.count(fc);
		q = q.multiselect(fc.alias("failureCause"), count.alias("count")).groupBy(fc).orderBy(c.desc(count));
		final TypedQuery<Tuple> tq = manager.createQuery(q);
		addLimit(limit, tq);
		final List<Tuple> counts = tq.getResultList();

		endTransaction(manager);

		// build result list with FailureCauses and counts
		final List<ObjectCountPair<String>> list = new ArrayList<ObjectCountPair<String>>(counts.size());
		for (final Tuple t : counts) {
			list.add(new ObjectCountPair<String>(t.get("failureCause", String.class),
					t.get("count", Long.class).intValue()));
		}
		return list;
	}

	@Override
	public List<ObjectCountPair<String>> getFailureCauseNames(GraphFilterBuilder filter) {
		final List<ObjectCountPair<FailureCause>> fcs = getNbrOfFailureCauses(filter);

		final List<ObjectCountPair<String>> result = new ArrayList<ObjectCountPair<String>>();
		for (final ObjectCountPair<FailureCause> f : fcs) {
			result.add(new ObjectCountPair<String>(f.getObject().getName(), f.getCount()));
		}
		return result;
	}

	@Override
	public long getNbrOfNullFailureCauses(GraphFilterBuilder filter) {
		final EntityManager manager = beginTransaction();
		final CriteriaBuilder c = manager.getCriteriaBuilder();
		final CriteriaQuery<Long> cq = c.createQuery(Long.class);
		final Root<Statistics> r = cq.from(Statistics.class);
		final CriteriaQuery<Long> query = getQuery(cq, r, manager, filter);
		final Predicate size = getNullFailureCauses(c, r, query);
		final Predicate where = query.getRestriction() == null ? size : c.and(query.getRestriction(), size);
		final Long result = manager.createQuery(query.select(c.count(r))
				.where(where)).getSingleResult();
		endTransaction(manager);
		return result;
	}

	/**
	 * @param c
	 * @param query
	 * @return
	 */
	private Predicate getNullFailureCauses(final CriteriaBuilder c, Root<Statistics> r, final CriteriaQuery<?> query) {
		final Expression<List<FailureCauseStatistics>> list = r.get(Statistics_.failureCauseStatisticsList);
		return c.equal(c.size(list), 0);
	}

	@Override
	public List<ObjectCountPair<String>> getNbrOfFailureCategoriesPerName(GraphFilterBuilder filter, int limit) {
		final List<ObjectCountPair<FailureCause>> fcs = getNbrOfFailureCauses(filter, limit);
		final Map<String, ObjectCountPair<String>> counts = new HashMap<String, ObjectCountPair<String>>();
		for (final ObjectCountPair<FailureCause> fc : fcs) {
			if (fc.getObject().getCategories() != null) {
				for (final String c : fc.getObject().getCategories()) {
					if (!counts.containsKey(c)) {
						counts.put(c, new ObjectCountPair<String>(c, 0));
					}
					counts.get(c).addCount(fc.getCount());
				}
			}
		}
		final List<ObjectCountPair<String>> result = new ArrayList<ObjectCountPair<String>>(counts.values());
		final Comparator<ObjectCountPair<?>> comparator = Collections
				.reverseOrder(new Comparator<ObjectCountPair<?>>() {
					@Override
					public int compare(ObjectCountPair<?> o1, ObjectCountPair<?> o2) {
						return Integer.valueOf(o1.getCount()).compareTo(Integer.valueOf(o2.getCount()));
					}
				});
		Collections.sort(result, comparator);
		return result;
	}

	@Override
	public Map<Integer, List<FailureCause>> getFailureCausesPerBuild(GraphFilterBuilder filter) {
		final EntityManager manager = beginTransaction();
		final TypedQuery<Statistics> q = getStatisticsQuery(manager, filter);
		final List<Statistics> stats = q.getResultList();

		final Collection<String> ids = new HashSet<String>();
		for (final Statistics s : stats) {
			for (final FailureCauseStatistics f : s.getFailureCauseStatisticsList()) {
				ids.add(f.getId());
			}
		}
		final Map<String, FailureCause> fcs = getFailureCauses(ids);

		final Map<Integer, List<FailureCause>> result = new HashMap<Integer, List<FailureCause>>();
		for (final Statistics s : stats) {
			final Integer build = s.getBuildNumber();
			if (!result.containsKey(build)) {
				result.put(build, new ArrayList<FailureCause>());
			}
			for (final FailureCauseStatistics f : s.getFailureCauseStatisticsList()) {
				result.get(build).add(fcs.get(f.getId()));
			}
		}
		endTransaction(manager);
		return result;
	}

	@Override
	public Map<TimePeriod, Double> getUnknownFailureCauseQuotaPerTime(int intervalSize, GraphFilterBuilder filter) {
		final EntityManager manager = beginTransaction();
		final CriteriaBuilder c = manager.getCriteriaBuilder();

		final CriteriaQuery<Tuple> cq1 = c.createTupleQuery();
		final Root<Statistics> r1 = cq1.from(Statistics.class);
		final CriteriaQuery<Tuple> q1 = getQuery(cq1, r1, manager, filter);
		final Map<TimePeriod, Integer> unknown = getFailedStatistics(manager, q1, r1, getNullFailureCauses(c, r1, q1),
				intervalSize);
		final CriteriaQuery<Tuple> cq2 = c.createTupleQuery();
		final Root<Statistics> r2 = cq2.from(Statistics.class);
		final CriteriaQuery<Tuple> q2 = getQuery(cq2, r2, manager, filter);
		final Map<TimePeriod, Integer> known = getFailedStatistics(manager, q2, r2,
				c.not(getNullFailureCauses(c, r2, q2)),
				intervalSize);
		endTransaction(manager);

		final Set<TimePeriod> periods = new HashSet<TimePeriod>(unknown.keySet());
		periods.addAll(known.keySet());

		final Map<TimePeriod, Double> nullFailureCauseQuotas = new HashMap<TimePeriod, Double>();
		for (final TimePeriod t : periods) {
			final int unknownCount = unknown.containsKey(t) ? unknown.get(t) : 0;
			final int knownCount = known.containsKey(t) ? known.get(t) : 0;
			double quota;
			if (unknownCount == 0) {
				quota = 0d;
			} else {
				quota = ((double) unknownCount) / (unknownCount + knownCount);
			}
			nullFailureCauseQuotas.put(t, quota);
		}
		return nullFailureCauseQuotas;
	}

	/**
	 * Generates a {@link TimePeriod} based on a MongoDB grouping aggregation result.
	 *
	 * @param result
	 *            the result to interpret
	 * @param intervalSize
	 *            the interval size, should be set to Calendar.HOUR_OF_DAY, Calendar.DATE or
	 *            Calendar.MONTH.
	 * @return TimePeriod
	 */
	private TimePeriod generateTimePeriodFromResult(Tuple result, int intervalSize) {
		final int month = result.get("month", Integer.class);
		final int year = result.get("year", Integer.class);

		final Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month - 1);
		// MongoDB timezone is UTC:
		c.setTimeZone(new SimpleTimeZone(0, "UTC"));

		TimePeriod period;
		if (intervalSize == Calendar.HOUR_OF_DAY) {
			final int dayOfMonth = result.get("day", Integer.class);
			c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
			final int hour = result.get("hour", Integer.class);
			c.set(Calendar.HOUR_OF_DAY, hour);

			period = new Hour(c.getTime());
		} else if (intervalSize == Calendar.DATE) {
			final int dayOfMonth = result.get("day", Integer.class);
			c.set(Calendar.DAY_OF_MONTH, dayOfMonth);

			period = new Day(c.getTime());
		} else {
			period = new Month(c.getTime());
		}
		return period;
	}

	private Map<TimePeriod, Integer> generateTimePeriods(List<Tuple> result, int intervalSize) {
		final Map<TimePeriod, Integer> map = new HashMap<TimePeriod, Integer>();
		for (final Tuple t : result) {
			map.put(generateTimePeriodFromResult(t, intervalSize), t.get("count", Long.class).intValue());
		}
		return map;
	}

	/**
	 *
	 * @param manager
	 * @param q
	 * @param p
	 * @param intervalSize
	 * @return
	 */
	private Map<TimePeriod, Integer> getFailedStatistics(EntityManager manager, CriteriaQuery<Tuple> q,
			Root<Statistics> r, Predicate p, int intervalSize) {
		final List<Selection<?>> select = new ArrayList<Selection<?>>();
		final List<Expression<?>> group = new ArrayList<Expression<?>>();

		final CriteriaQuery<Tuple> query = getFailedStatisticsQuery(manager, q, r, p, intervalSize, select, group)
				.multiselect(select).groupBy(group);
		return generateTimePeriods(manager.createQuery(query).getResultList(), intervalSize);
	}

	/**
	 * @param manager
	 * @param q
	 * @param p
	 * @param intervalSize
	 * @param select
	 * @param group
	 * @return
	 */
	private CriteriaQuery<Tuple> getFailedStatisticsQuery(EntityManager manager, CriteriaQuery<Tuple> q,
			Root<Statistics> r, Predicate p,
			int intervalSize, final List<Selection<?>> select, final List<Expression<?>> group) {
		final CriteriaBuilder c = manager.getCriteriaBuilder();

		final Predicate failed = c.notEqual(r.get(Statistics_.result), "SUCCESS");
		final Predicate pred = c.and(p, failed);
		final Predicate where = q.getRestriction() == null ? pred : c.and(q.getRestriction(), pred);
		final Path<Date> st = r.get(Statistics_.startingTime);
		if (intervalSize == Calendar.HOUR_OF_DAY) {
			final Expression<Integer> hour = intFunc(c, "HOUR", st);
			select.add(hour.alias("hour"));
			group.add(hour);
		}
		if (intervalSize == Calendar.HOUR_OF_DAY || intervalSize == Calendar.DATE) {
			final Expression<Integer> day = intFunc(c, "DAYOFMONTH", st);
			select.add(day.alias("day"));
			group.add(day);
		}
		final Expression<Integer> month = intFunc(c, "MONTH", st);
		select.add(month.alias("month"));
		group.add(month);
		final Expression<Integer> year = intFunc(c, "YEAR", st);
		select.add(year.alias("year"));
		group.add(year);

		select.add(c.count(r).alias("count"));
		return q.where(where);
	}

	private Expression<Integer> intFunc(CriteriaBuilder c, String name, Path<?> attribute) {
		return c.function(name, Integer.class, attribute);
	}

	@Override
	public List<FailureCauseTimeInterval> getFailureCausesPerTime(int intervalSize, GraphFilterBuilder filter,
			boolean byCategories) {
		final EntityManager manager = beginTransaction();
		final CriteriaBuilder c = manager.getCriteriaBuilder();

		final CriteriaQuery<Tuple> cq = c.createTupleQuery();
		final Root<Statistics> stat = cq.from(Statistics.class);
		final CriteriaQuery<Tuple> q1 = getQuery(cq, stat, manager, filter);
		final Root<FailureCause> fc = q1.from(FailureCause.class);
		final Path<String> fcId = fc.get(FailureCause_.id);
		final Predicate id = c.equal(fcId,
				stat.join(Statistics_.failureCauseStatisticsList).get(FailureCauseStatistics_.id));

		final List<Selection<?>> select = new ArrayList<Selection<?>>();
		final List<Expression<?>> group = new ArrayList<Expression<?>>();
		if (byCategories) {
			final ListJoin<FailureCause, String> category = fc.join(FailureCause_.categories);
			select.add(category.alias("category"));
			group.add(category);
		} else {
			select.add(fcId.alias("id"));
			final Path<String> name = fc.get(FailureCause_.name);
			select.add(name.alias("name"));
			group.add(fcId);
			group.add(name);
		}
		final List<Tuple> result = manager
				.createQuery(getFailedStatisticsQuery(manager, q1, stat, id, intervalSize, select,
						group).multiselect(select).groupBy(group))
				.getResultList();
		endTransaction(manager);

		final List<FailureCauseTimeInterval> failureCauseIntervals = new ArrayList<FailureCauseTimeInterval>();
		final Map<MultiKey, FailureCauseTimeInterval> categoryTable = new HashMap<MultiKey, FailureCauseTimeInterval>();
		for (final Tuple t : result) {
			final TimePeriod period = generateTimePeriodFromResult(t, intervalSize);
			final int number = t.get("count", Long.class).intValue();
			if (byCategories) {
				final String category = t.get("category", String.class);

				final MultiKey multiKey = new MultiKey(category, period);
				FailureCauseTimeInterval interval = categoryTable.get(multiKey);
				if (interval == null) {
					interval = new FailureCauseTimeInterval(period, category, number);
					categoryTable.put(multiKey, interval);
					failureCauseIntervals.add(interval);
				} else {
					interval.addNumber(number);
				}
			} else {
				final FailureCauseTimeInterval timeInterval = new FailureCauseTimeInterval(period,
						t.get("name", String.class), t.get("id", String.class), number);
				failureCauseIntervals.add(timeInterval);
			}
		}
		return failureCauseIntervals;
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
