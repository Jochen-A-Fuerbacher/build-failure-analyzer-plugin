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

import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.kohsuke.stapler.DataBoundConstructor;

import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;
import com.sonyericsson.jenkins.plugins.bfa.statistics.Statistics;
import com.sonyericsson.jenkins.plugins.bfa.Messages;

import hudson.Extension;
import hudson.model.Descriptor;
import hudson.util.Secret;
import jenkins.model.Jenkins;

public class SqlDBKnowledgeBase extends KnowledgeBase {
	
    private static final Logger logger = Logger.getLogger(SqlDBKnowledgeBase.class.getName());
	
    private String host;
    private int port;
    private String dbName;
    private String userName;
    private Secret password;
    private boolean enableStatistics;
    private boolean successfulLogging;
    
    /**
     * Getter for the SQL DB user name.
     * @return the user name.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Getter for the SQL DB password.
     * @return the password.
     */
    public Secret getPassword() {
        return password;
    }

   /**
     * Getter for the host value.
     * @return the host string.
     */
    public String getHost() {
        return host;
    }

    /**
     * Getter for the port value.
     * @return the port number.
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Getter for the database name value.
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
     * @param host the host to connect to.
     * @param port the port to connect to.
     * @param dbName the database name to connect to.
     * @param userName the user name for the database.
     * @param password the password for the database.
     * @param enableStatistics if statistics logging should be enabled or not.
     * @param successfulLogging if all builds should be logged to the statistics DB
     */
    @DataBoundConstructor
    public SqlDBKnowledgeBase(String host, int port, String dbName, String userName, Secret password,
                                boolean enableStatistics, boolean successfulLogging) {
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
		return Jenkins.getInstance().getDescriptorByType(SqlDBKnowledgeBaseDescriptor.class);
	}

	@Override
	public Collection<FailureCause> getCauses() throws Exception {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FailureCause addCause(FailureCause cause) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FailureCause removeCause(String id) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FailureCause saveCause(FailureCause cause) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void convertFrom(KnowledgeBase oldKnowledgeBase) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> getCategories() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean equals(KnowledgeBase oldKnowledgeBase) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public void saveStatistics(Statistics stat) throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * Descriptor for {@link SqlDBKnowledgeBase}.
	 */
	@Extension
	public static class SqlDBKnowledgeBaseDescriptor extends KnowledgeBaseDescriptor {

		@Override
		public String getDisplayName() {
			return Messages.SqlDBKnowledgeBase_DisplayName();
		}
	}

}
