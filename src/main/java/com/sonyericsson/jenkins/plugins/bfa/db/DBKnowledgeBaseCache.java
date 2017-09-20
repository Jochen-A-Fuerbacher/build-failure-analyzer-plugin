package com.sonyericsson.jenkins.plugins.bfa.db;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.MongoException;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;

/**
 * @author rlamberti
 *
 */
public abstract class DBKnowledgeBaseCache {
	private static final long CACHE_UPDATE_INTERVAL = 60000;

	private Semaphore shouldUpdate;
	private UpdateThread updaterThread;
	private Timer timer;
	private TimerTask timerTask;
	private List<FailureCause> cachedFailureCauses;
	private List<String> cachedCategories;

	private final Logger logger = Logger.getLogger(MongoDBKnowledgeBase.class.getName());

	/**
	 * Run when the cache, including the update mechanism, should start running.
	 */
	public void start() {
		shouldUpdate = new Semaphore();
		updaterThread = new UpdateThread();
		updaterThread.start();
		timer = new Timer();
		timerTask = new TimerTask() {
			@Override
			public void run() {
				shouldUpdate.release();
			}
		};
		timer.scheduleAtFixedRate(timerTask, 0, CACHE_UPDATE_INTERVAL);
	}

	/**
	 * Run when we want to shut down the cache.
	 */
	public void stop() {
		timer.cancel();
		timer = null;
		timerTask = null;
		updaterThread.stopThread();
		updaterThread = null;
	}

	/**
	 * Getter for the cachedFailureCauses.
	 *
	 * @return the causes.
	 */
	public List<FailureCause> getCauses() {
		return cachedFailureCauses;
	}

	/**
	 * Getter for the categories of all FailureCauses.
	 *
	 * @return the categories.
	 */
	public List<String> getCategories() {
		return cachedCategories;
	}

	/**
	 * Signal that an update of the Cache should be made.
	 */
	public void updateCache() {
		if (shouldUpdate != null) {
			shouldUpdate.release();
		}
	}

	protected abstract List<FailureCause> updateCausesList();

	protected abstract List<String> updateCategories();

	/**
	 * The thread responsible for updating the MongoDB cache.
	 */
	protected class UpdateThread extends Thread {
		private volatile boolean stop = false;

		@Override
		public void run() {
			while (!stop) {
				try {
					shouldUpdate.acquire();
					if (stop) {
						break;
					}
					cachedFailureCauses = updateCausesList();
					cachedCategories = updateCategories();
					logger.fine("Updated FailureCause KnowledgeDB Cache.");
				} catch (final MongoException e) {
					logger.log(Level.SEVERE, "MongoException caught when updating cache: " + e);
				} catch (final InterruptedException e) {
					logger.log(Level.WARNING, "Updater thread interrupted", e);
				}
			}
		}

		/**
		 * Stops the execution of this thread.
		 */
		protected void stopThread() {
			stop = true;
			shouldUpdate.release();
		}
	}
}