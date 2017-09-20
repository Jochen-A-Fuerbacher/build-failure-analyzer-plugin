package com.sonyericsson.jenkins.plugins.bfa.db;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import javax.naming.AuthenticationException;

import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;

/**
 * @author rlamberti
 *
 */
public abstract class CachedKnowledgeBase extends KnowledgeBase {
	private transient DBKnowledgeBaseCache cache;

	private static final Logger logger = Logger.getLogger(CachedKnowledgeBase.class.getName());

	@Override
	public synchronized void stop() {
		if (cache != null) {
			cache.stop();
			cache = null;
		}
	}

	@Override
	public synchronized void start() throws Exception {
		initCache();
	}

	/**
	 * @see KnowledgeBase#getCauses() Can throw MongoException if unknown fields exist in the
	 *      database.
	 * @return the full list of causes.
	 * @throws UnknownHostException
	 *             if a connection to the host cannot be made.
	 * @throws AuthenticationException
	 *             if we cannot authenticate towards the database.
	 */
	@Override
	public Collection<FailureCause> getCauses() throws Exception {
		initCache();
		return cache.getCauses();
	}

	@Override
	public List<String> getCategories() throws Exception {
		initCache();
		return cache.getCategories();
	}

	@Override
	public FailureCause addCause(FailureCause cause) throws Exception {
		return addCause(cause, true);
	}

	/**
	 * Does not update the cache, used when we know we will have a lot of save/add calls all at
	 * once, e.g. during a convert.
	 *
	 * @param cause
	 *            the FailureCause to add.
	 * @param doUpdate
	 *            true if a cache update should be made, false if not.
	 *
	 * @return the added FailureCause.
	 *
	 * @throws UnknownHostException
	 *             If a connection to the Mongo database cannot be made.
	 * @throws javax.naming.AuthenticationException
	 *             if we cannot authenticate towards the database.
	 *
	 * @see MongoDBKnowledgeBase#addCause(FailureCause)
	 */
	protected FailureCause addCause(FailureCause cause, boolean doUpdate) throws Exception {
		final FailureCause persisted = persistCause(cause);
		logger.info("Added failure cause '" + cause.getName() + "' with id " + cause.getId());
		if (doUpdate) {
			updateCache();
		}
		return persisted;
	}

	@Override
	public FailureCause saveCause(FailureCause cause) throws Exception {
		return saveCause(cause, true);
	}

	/**
	 * Does not update the cache, used when we know we will have a lot of save/add calls all at
	 * once, e.g. during a convert.
	 *
	 * @param cause
	 *            the FailureCause to save.
	 * @param doUpdate
	 *            true if a cache update should be made, false if not.
	 *
	 * @return the saved FailureCause.
	 *
	 * @throws UnknownHostException
	 *             If a connection to the Mongo database cannot be made.
	 * @throws AuthenticationException
	 *             if we cannot authenticate towards the database.
	 *
	 * @see MongoDBKnowledgeBase#saveCause(FailureCause)
	 */
	protected FailureCause saveCause(FailureCause cause, boolean doUpdate) throws Exception {
		final FailureCause c = mergeCause(cause);
		logger.info("Updated failure cause '" + c.getName() + "' with id " + c.getId());
		if (doUpdate) {
			updateCache();
		}
		return c;
	}

	protected void updateCache() throws Exception {
		initCache();
		cache.updateCache();
	}

	private synchronized void initCache() throws Exception {
		if (cache == null) {
			cache = buildCache();
			cache.start();
		}
	}

	protected abstract DBKnowledgeBaseCache buildCache() throws Exception;

	protected abstract FailureCause persistCause(FailureCause cause) throws Exception;

	protected abstract FailureCause mergeCause(FailureCause cause) throws Exception;
}
