/*
 * The MIT License
 *
 * Copyright 2012 Sony Mobile Communications AB. All rights reserved.
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

package com.sonyericsson.jenkins.plugins.bfa.statistics;

import java.util.Date;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * The statistics object which will be logged.
 * @author Tomas Westling &lt;tomas.westling@sonymobile.com&gt;
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
public class Statistics {

	@Id
	@Column(name="ID")
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private long id;

	@Column(name="PROJECTNAME")
    private String projectName;
	@Column(name="BUILDNUMBER")
    private int buildNumber;
	@Column(name="DISPLAYNAME")
    private String displayName;
	@Column(name="STARTINGTIME")
	@Temporal(TemporalType.TIMESTAMP)
    private Date startingTime;
	@Column(name="DURATION")
    private long duration;
	@Column(name="TRIGGERCAUSES")
	@ElementCollection(fetch = FetchType.EAGER)
    private List<String> triggerCauses;
	@Column(name="SLAVE")
    private String slave;
	@Column(name="MASTER")
    private String master;
	@Column(name="TIMEZONEOFFSET")
    private int timeZoneOffset;
	@Column(name="RESULT")
    private String result;
	@Column(name="UPSTREAMCAUSE")
    private UpstreamCause upstreamCause;
	@OneToMany(cascade = CascadeType.ALL)
    private List<FailureCauseStatistics> failureCauseStatisticsList;

    /**
     * Getter for the project name.
     * @return the project name.
     */
    public String getProjectName() {
        return projectName;
    }

     /**
     * Getter for the build number.
     * @return the build number.
     */
    public int getBuildNumber() {
        return buildNumber;
    }

    /**
     * Getter for the build display name.
     * @return the build display name.
     */
    public String getDisplayName() {
        return displayName;
    }

     /**
     * Getter for the starting time.
     * @return the starting time.
     */
    public Date getStartingTime() {
        if (startingTime == null) {
            return null;
        }
        return new Date(startingTime.getTime());
    }

     /**
     * Getter for the duration.
     * @return the duration.
     */
    public long getDuration() {
        return duration;
    }

     /**
     * Getter for the build cause that triggered the build.
     * @return the causes as a list.
     */
    public List<String> getTriggerCauses() {
        return triggerCauses;
    }

     /**
     * Getter for the host name of the slave.
     * @return the host name of the slave.
     */
    public String getSlaveHostName() {
        return slave;
    }

     /**
     * Getter for the Jenkins master.
     * @return the master.
     */
    public String getMaster() {
        return master;
    }

     /**
     * Getter for the time zone offset in milliseconds.
     * @return the time zone offset.
     */
    public int getTimeZoneOffset() {
        return timeZoneOffset;
    }

    /**
     * Getter for the result of the build.
      * @return the result.
     */
    public String getResult() {
        return result;
    }

    /**
     * Getter for the upstream cause description.
     * @return the upstream cause description.
     */
    public UpstreamCause getUpstreamCause() {
        return upstreamCause;
    }

    /**
     * Getter for the List of statistics for FailureCauses.
     * @return the list.
     */
    public List<FailureCauseStatistics> getFailureCauseStatisticsList() {
        return failureCauseStatisticsList;
    }

    /**
     * Standard/JSON constructor.
     * @param projectName the project name.
     * @param buildNumber the build number.
     * @param displayName the build display name.
     * @param startingTime the starting time.
     * @param duration the duration.
     * @param triggerCauses the causes that triggered this build.
     * @param nodeName the name of the node this build ran on.
     * @param master the master this build ran on.
     * @param timeZoneOffset the time zone offset.
     * @param result the result of the build.
     * @param upstreamCause the upstream cause of the current build, if any.
     * @param failureCauseStatistics the statistics for the FailureCauses.
     */
    @JsonCreator
    public Statistics(@JsonProperty("projectName")    String projectName,
                      @JsonProperty("buildNumber")    int buildNumber,
                      @JsonProperty("displayName")    String displayName,
                      @JsonProperty("startingTime")   Date startingTime,
                      @JsonProperty("duration")       long duration,
                      @JsonProperty("triggerCauses")  List<String> triggerCauses,
                      @JsonProperty("slaveHostName")  String nodeName,
                      @JsonProperty("master")         String master,
                      @JsonProperty("timeZoneOffset") int timeZoneOffset,
                      @JsonProperty("result")         String result,
                      @JsonProperty("upstreamCause")  UpstreamCause upstreamCause,
                      @JsonProperty("failureCauses")  List<FailureCauseStatistics> failureCauseStatistics) {
        this.projectName = projectName;
        this.buildNumber = buildNumber;
        this.displayName = displayName;
        if (startingTime == null) {
            this.startingTime = null;
        } else {
            this.startingTime = new Date(startingTime.getTime());
        }
        this.duration = duration;
        this.triggerCauses = triggerCauses;
        this.slave = nodeName;
        this.master = master;
        this.timeZoneOffset = timeZoneOffset;
        this.result = result;
        this.upstreamCause = upstreamCause;
        this.failureCauseStatisticsList = failureCauseStatistics;
    }

        /**
         * @deprecated, kept for backwards compatibility.
         * @param projectName the project name.
         * @param buildNumber the build number.
         * @param startingTime the starting time.
         * @param duration the duration.
         * @param triggerCauses the causes that triggered this build.
         * @param nodeName the name of the node this build ran on.
         * @param master the master this build ran on.
         * @param timeZoneOffset the time zone offset.
         * @param result the result of the build.
         * @param failureCauseStatistics the statistics for the FailureCauses.
         */
    @Deprecated
    public Statistics(String projectName,
                      int buildNumber,
                      Date startingTime,
                      long duration,
                      List<String> triggerCauses,
                      String nodeName,
                      String master,
                      int timeZoneOffset,
                      String result,
                      List<FailureCauseStatistics> failureCauseStatistics) {
        this(projectName, buildNumber, null, startingTime, duration, triggerCauses, nodeName, master, timeZoneOffset,
             result, null, failureCauseStatistics);
    }

    /**
	 * Default constructor. <strong>Do not use this unless you are a serializer.</strong>
	 */
    public Statistics() {
    }
}
