/*
 * The MIT License
 *
 * Copyright 2012 Sony Mobile Communications Inc. All rights reserved.
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

import static java.util.Arrays.asList;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.AuthenticationException;

import org.apache.commons.collections.keyvalue.MultiKey;
import org.bson.types.ObjectId;
import org.jfree.data.time.Day;
import org.jfree.data.time.Hour;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimePeriod;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.sonyericsson.jenkins.plugins.bfa.Messages;
import com.sonyericsson.jenkins.plugins.bfa.graphs.FailureCauseTimeInterval;
import com.sonyericsson.jenkins.plugins.bfa.graphs.GraphFilterBuilder;
import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;
import com.sonyericsson.jenkins.plugins.bfa.model.indication.FoundIndication;
import com.sonyericsson.jenkins.plugins.bfa.statistics.FailureCauseStatistics;
import com.sonyericsson.jenkins.plugins.bfa.statistics.Statistics;
import com.sonyericsson.jenkins.plugins.bfa.statistics.UpstreamCause;
import com.sonyericsson.jenkins.plugins.bfa.utils.BfaUtils;
import com.sonyericsson.jenkins.plugins.bfa.utils.ObjectCountPair;

import hudson.Extension;
import hudson.Util;
import hudson.model.Descriptor;
import hudson.model.Run;
import hudson.util.FormValidation;
import hudson.util.Secret;
import jenkins.model.Jenkins;
import net.vz.mongodb.jackson.DBCursor;
import net.vz.mongodb.jackson.JacksonDBCollection;
import net.vz.mongodb.jackson.WriteResult;

/**
 * Handling of the MongoDB way of saving the knowledge base.
 *
 * @author Tomas Westling &lt;tomas.westling@sonyericsson.com&gt;
 */
public class MongoDBKnowledgeBase extends CachedKnowledgeBase {

    private static final long serialVersionUID = 4984133048405390951L;
    /**The name of the cause collection in the database.*/
    public static final String COLLECTION_NAME = "failureCauses";
    /**The name of the statistics collection in the database.*/
    public static final String STATISTICS_COLLECTION_NAME = "statistics";
    private static final int MONGO_DEFAULT_PORT = 27017;
    /**
     * Query to single out documents that doesn't have a "removed" property
     */
    static final BasicDBObject NOT_REMOVED_QUERY = new BasicDBObject("_removed", new BasicDBObject("$exists", false));
    private static final Logger logger = Logger.getLogger(MongoDBKnowledgeBase.class.getName());

    private transient Mongo mongo;
    private transient DB db;
    private transient DBCollection collection;
    private transient DBCollection statisticsCollection;
    private transient JacksonDBCollection<FailureCause, String> jacksonCollection;
    private transient JacksonDBCollection<Statistics, String> jacksonStatisticsCollection;

    private final String host;
    private final int port;
    private final String dbName;
    private final String userName;
    private final Secret password;
    private final boolean enableStatistics;
    private final boolean successfulLogging;

    /**
     * Getter for the MongoDB user name.
     * @return the user name.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Getter for the MongoDB password.
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
    public MongoDBKnowledgeBase(String host, int port, String dbName, String userName, Secret password,
                                boolean enableStatistics, boolean successfulLogging) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.userName = userName;
        this.password = password;
        this.enableStatistics = enableStatistics;
        this.successfulLogging = successfulLogging;
    }

    /**
     * Initializes the cache if it is null.
     * @throws UnknownHostException if we cannot connect to the database.
     * @throws AuthenticationException if we cannot authenticate towards the database.
     */
    @Override
	protected DBKnowledgeBaseCache buildCache() throws UnknownHostException, AuthenticationException {
    	return new MongoDBKnowledgeBaseCache(getJacksonCollection());
    }

    /**
     * @see KnowledgeBase#getCauseNames()
     * Can throw MongoException if unknown fields exist in the database.
     * @return the full list of the names and ids of the causes..
     * @throws UnknownHostException if a connection to the host cannot be made.
     * @throws AuthenticationException if we cannot authenticate towards the database.
     */
    @Override
    public Collection<FailureCause> getCauseNames() throws UnknownHostException, AuthenticationException {
        final List<FailureCause> list = new LinkedList<FailureCause>();
        final DBObject keys = new BasicDBObject();
        keys.put("name", 1);
        final DBCursor<FailureCause> dbCauses =  getJacksonCollection().find(NOT_REMOVED_QUERY, keys);
        while (dbCauses.hasNext()) {
            list.add(dbCauses.next());
        }
        return list;

    }

    @Override
    public Collection<FailureCause> getShallowCauses() throws Exception {
        final List<FailureCause> list = new LinkedList<FailureCause>();
        final DBObject keys = new BasicDBObject();
        keys.put("name", 1);
        keys.put("description", 1);
        keys.put("categories", 1);
        keys.put("comment", 1);
        keys.put("modifications", 1);
        keys.put("lastOccurred", 1);
        final BasicDBObject orderBy = new BasicDBObject("name", 1);
        DBCursor<FailureCause> dbCauses =  getJacksonCollection().find(NOT_REMOVED_QUERY, keys);
        dbCauses = dbCauses.sort(orderBy);
        while (dbCauses.hasNext()) {
            list.add(dbCauses.next());
        }
        return list;
    }

    @Override
    public FailureCause getCause(String id) throws UnknownHostException, AuthenticationException {
        FailureCause returnCase = null;
        try {
            returnCase = getJacksonCollection().findOneById(id);
        } catch (final IllegalArgumentException e) {
         logger.fine("Could not find the id, returning null for id: " + id);
            return returnCase;
        }
        return returnCase;
    }

    @Override
    public FailureCause removeCause(String id) throws Exception {
        final BasicDBObject idq = new BasicDBObject("_id", new ObjectId(id));
        final BasicDBObject removedInfo = new BasicDBObject("timestamp", new Date());
        removedInfo.put("by", Jenkins.getAuthentication().getName());
        final BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("_removed", removedInfo));
        final FailureCause modified = getJacksonCollection().findAndModify(idq, null, null, false, update, true, false);
        updateCache();
        return modified;
    }

    @Override
    protected FailureCause persistCause(FailureCause cause) throws AuthenticationException, MongoException, UnknownHostException {
    	final WriteResult<FailureCause, String> result = getJacksonCollection().insert(cause);
    	return result.getSavedObject();
    }

    @Override
    protected FailureCause mergeCause(FailureCause cause) throws Exception {
    	final WriteResult<FailureCause, String> result =  getJacksonCollection().save(cause);
    	return result.getSavedObject();
    }

    @Override
    public void convertFrom(KnowledgeBase oldKnowledgeBase) throws Exception {
        if (oldKnowledgeBase instanceof MongoDBKnowledgeBase) {
            convertFromAbstract(oldKnowledgeBase);
            convertRemoved((MongoDBKnowledgeBase)oldKnowledgeBase);
        } else {
            for (final FailureCause cause : oldKnowledgeBase.getCauseNames()) {
                try {
                	// get cause with fetch indications, modifications, etc
                	final FailureCause fullCause = oldKnowledgeBase.getCause(cause.getId());
                    //try finding the id in the knowledgebase, if so, update it.
                    if (getCause(cause.getId()) != null) {
                        //doing all the additions to the database first and then fetching to the cache only once.
                        saveCause(fullCause, false);
                    //if not found, add a new.
                    } else {
                        fullCause.setId(null);
                        addCause(fullCause, false);
                    }
                  //Safety net for the case that Mongo should throw anything if the id has a really weird form.
                } catch (final MongoException e) {
                    cause.setId(null);
                    addCause(cause, false);
                }
            }
            updateCache();
        }
    }

    /**
     * Copies all causes flagged as removed from the old database to this one.
     *
     * @param oldKnowledgeBase the old database.
     * @throws Exception if something goes wrong.
     */
    protected void convertRemoved(MongoDBKnowledgeBase oldKnowledgeBase) throws Exception {
        final List<DBObject> removed = oldKnowledgeBase.getRemovedCauses();
        final DBCollection dbCollection = getJacksonCollection().getDbCollection();
        for (final DBObject obj : removed) {
            dbCollection.save(obj);
        }
    }

    /**
     * Gets all causes flagged as removed in a "raw" JSON format.
     *
     * @return the list of removed causes.
     * @throws Exception if so.
     */
    protected List<DBObject> getRemovedCauses() throws Exception {
        final BasicDBObject query = new BasicDBObject("_removed", new BasicDBObject("$exists", true));
        final com.mongodb.DBCursor dbCursor = getJacksonCollection().getDbCollection().find(query);
        final List<DBObject> removed = new LinkedList<DBObject>();
        while (dbCursor.hasNext()) {
            removed.add(dbCursor.next());
        }
        return removed;
    }

    @Override
    public boolean equals(KnowledgeBase oldKnowledgeBase) {
        if (getClass().isInstance(oldKnowledgeBase)) {
            final MongoDBKnowledgeBase oldMongoDBKnowledgeBase = (MongoDBKnowledgeBase)oldKnowledgeBase;
            return equals(oldMongoDBKnowledgeBase.getHost(), host)
                    && oldMongoDBKnowledgeBase.getPort() == port
                    && equals(oldMongoDBKnowledgeBase.getDbName(), dbName)
                    && equals(oldMongoDBKnowledgeBase.getUserName(), userName)
                    && equals(oldMongoDBKnowledgeBase.getPassword(), password)
                    && this.enableStatistics == oldMongoDBKnowledgeBase.enableStatistics
                    && this.successfulLogging == oldMongoDBKnowledgeBase.successfulLogging;
        } else {
            return false;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof KnowledgeBase) {
            return this.equals((KnowledgeBase)other);
        } else {
            return false;
        }
    }

    /**
     * Checks if two objects equal each other, both being null counts as being equal.
     * @param firstObject the firstObject.
     * @param secondObject the secondObject.
     * @return true if equal or both null, false otherwise.
     */
    public static boolean equals(Object firstObject, Object secondObject) {
        if (firstObject == null) {
            if (secondObject == null) {
                return true;
            }
            return false;
        }
        if (secondObject == null) {
            return false;
        }
        return secondObject.equals(firstObject);
    }

    @Override
    public int hashCode() {
        //Making checkstyle happy.
        return getClass().getName().hashCode();
    }

    @Override
    public boolean isStatisticsEnabled() {
        return enableStatistics;
    }

    @Override
    public boolean isSuccessfulLoggingEnabled() {
        return successfulLogging;
    }

    @Override
    public void saveStatistics(Statistics stat) throws UnknownHostException, AuthenticationException {
        final DBObject object = new BasicDBObject();
        object.put("projectName", stat.getProjectName());
        object.put("buildNumber", stat.getBuildNumber());
        object.put("displayName", stat.getDisplayName());
        object.put("master", stat.getMaster());
        object.put("slaveHostName", stat.getSlaveHostName());
        object.put("startingTime", stat.getStartingTime());
        object.put("duration", stat.getDuration());
        object.put("timeZoneOffset", stat.getTimeZoneOffset());
        object.put("triggerCauses", stat.getTriggerCauses());
        DBObject cause = null;
        if (stat.getUpstreamCause() != null) {
            cause = new BasicDBObject();
            final UpstreamCause upstreamCause = stat.getUpstreamCause();
            cause.put("project", upstreamCause.getUpstreamProject());
            cause.put("build", upstreamCause.getUpstreamBuild());
        }
        object.put("upstreamCause", cause);
        object.put("result", stat.getResult());
        final List<FailureCauseStatistics> failureCauseStatisticsList = stat.getFailureCauseStatisticsList();
        addFailureCausesToDBObject(object, failureCauseStatisticsList);

        getStatisticsCollection().insert(object);
       }

    @Override
    public List<Statistics> getStatistics(GraphFilterBuilder filter, int limit)
            throws UnknownHostException, AuthenticationException {
        final DBObject matchFields = generateMatchFields(filter);
        DBCursor<Statistics> dbCursor = getJacksonStatisticsCollection().find(matchFields);
        final BasicDBObject buildNumberDescending = new BasicDBObject("buildNumber", -1);
        dbCursor = dbCursor.sort(buildNumberDescending);
        if (limit > 0) {
            dbCursor = dbCursor.limit(limit);
        }
        return dbCursor.toArray();
    }

    @Override
    public long getNbrOfNullFailureCauses(GraphFilterBuilder filter) {
        final DBObject matchFields = generateMatchFields(filter);
        matchFields.put("failureCauses", null);

        try {
            return getStatisticsCollection().count(matchFields);
        } catch (final Exception e) {
            logger.fine("Unable to get number of null failure causes");
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public Map<TimePeriod, Double> getUnknownFailureCauseQuotaPerTime(int intervalSize, GraphFilterBuilder filter) {
        final Map<TimePeriod, Integer> unknownFailures = new HashMap<TimePeriod, Integer>();
        final Map<TimePeriod, Integer> knownFailures = new HashMap<TimePeriod, Integer>();
        final Set<TimePeriod> periods = new HashSet<TimePeriod>();

        final DBObject matchFields = generateMatchFields(filter);
        final DBObject match = new BasicDBObject("$match", matchFields);

        // Use $project to change all null failurecauses to 'false' since
        // it's not possible to group by 'null':
        final DBObject projectFields = new BasicDBObject();
        projectFields.put("startingTime", 1);
        final DBObject nullToFalse = new BasicDBObject("$ifNull", asList("$failureCauses", false));
        projectFields.put("failureCauses", nullToFalse);
        final DBObject project = new BasicDBObject("$project", projectFields);

        // Group by date and false/non false failure causes:
        final DBObject idFields = generateTimeGrouping(intervalSize);
        final DBObject checkNullFailureCause = new BasicDBObject("$eq", asList("$failureCauses", false));
        idFields.put("isNullFailureCause", checkNullFailureCause);
        final DBObject groupFields = new BasicDBObject();
        groupFields.put("_id", idFields);
        groupFields.put("number", new BasicDBObject("$sum", 1));
        final DBObject group = new BasicDBObject("$group", groupFields);

        AggregationOutput output;
        try {
            output = getStatisticsCollection().aggregate(match, project, group);
            for (final DBObject result : output.results()) {
                final DBObject groupedAttrs = (DBObject)result.get("_id");
                final TimePeriod period = generateTimePeriodFromResult(result, intervalSize);
                periods.add(period);
                final int number = (Integer)result.get("number");
                final boolean isNullFailureCause = (Boolean)groupedAttrs.get("isNullFailureCause");
                if (isNullFailureCause) {
                    unknownFailures.put(period, number);
                } else {
                    knownFailures.put(period, number);
                }
            }
        } catch (final Exception e) {
            logger.fine("Unable to get unknown failure cause quota per time");
            e.printStackTrace();
        }
        final Map<TimePeriod, Double> nullFailureCauseQuotas = new HashMap<TimePeriod, Double>();
        for (final TimePeriod timePeriod : periods) {
            int unknownFailureCount = 0;
            int knownFailureCount = 0;
            if (unknownFailures.containsKey(timePeriod)) {
                unknownFailureCount = unknownFailures.get(timePeriod);
            }
            if (knownFailures.containsKey(timePeriod)) {
                knownFailureCount = knownFailures.get(timePeriod);
            }
            double quota;
            if (unknownFailureCount == 0) {
                quota = 0d;
            } else {
                quota = ((double)unknownFailureCount) / (unknownFailureCount + knownFailureCount);
            }
            nullFailureCauseQuotas.put(timePeriod, quota);
        }
        return nullFailureCauseQuotas;
    }

    @Override
    public List<ObjectCountPair<String>> getNbrOfFailureCausesPerId(GraphFilterBuilder filter, int maxNbr) {
        final List<ObjectCountPair<String>> nbrOfFailureCausesPerId = new ArrayList<ObjectCountPair<String>>();
        final DBObject matchFields = generateMatchFields(filter);
        final DBObject match = new BasicDBObject("$match", matchFields);

        final DBObject unwind = new BasicDBObject("$unwind", "$failureCauses");

        final DBObject groupFields = new BasicDBObject();
        groupFields.put("_id", "$failureCauses.failureCause");
        groupFields.put("number", new BasicDBObject("$sum", 1));
        final DBObject group = new BasicDBObject("$group", groupFields);

        final DBObject sort = new BasicDBObject("$sort", new BasicDBObject("number", -1));

        DBObject limit = null;
        if (maxNbr > 0) {
            limit = new BasicDBObject("$limit", maxNbr);
        }

        AggregationOutput output;
        try {
            if (limit == null) {
                output = getStatisticsCollection().aggregate(match, unwind, group, sort);
            } else {
                output = getStatisticsCollection().aggregate(match, unwind, group, sort, limit);
            }
            for (final DBObject result : output.results()) {
                final DBRef failureCauseRef = (DBRef)result.get("_id");
                if (failureCauseRef != null) {
                    final Integer number = (Integer)result.get("number");
                    final String id = failureCauseRef.getId().toString();
                    nbrOfFailureCausesPerId.add(new ObjectCountPair<String>(id, number));
                }
            }
        } catch (final Exception e) {
            logger.fine("Unable to get failure causes per id");
            e.printStackTrace();
        }

        return nbrOfFailureCausesPerId;
    }

    @Override
    public Date getLatestFailureForCause(String id) {

        final DBObject causeToMatch = new BasicDBObject("$ref", "failureCauses");
        causeToMatch.put("$id", new ObjectId(id));

        final DBObject causeList = new BasicDBObject("failureCauses.failureCause", causeToMatch);

        final DBObject match = new BasicDBObject("$match", causeList);
        final DBObject sort = new BasicDBObject("$sort", new BasicDBObject("startingTime", -1));
        final DBObject limit = new BasicDBObject("$limit", 1);

        AggregationOutput output;
        try {
            output = getStatisticsCollection().aggregate(match, sort, limit);

            for (final DBObject result : output.results()) {
                final Date startingTime = (Date)result.get("startingTime");

                if (startingTime != null) {
                    return startingTime;
                }
            }
        } catch (final Exception e) {
            logger.log(Level.WARNING, "Failed getting latest failure of cause", e);
        }

        return null;
    }

    @Override
    public Date getCreationDateForCause(String id) {
        Date creationDate;
        try {
            //Get the creation date using time information in MongoDB id:
            creationDate = new Date(new ObjectId(id).getTime());
        } catch (final IllegalArgumentException e) {
            logger.log(Level.WARNING, "Could not retrieve original modification", e);
            creationDate = new Date(0);
        }
        return creationDate;
    }

    @Override
    public void updateLastSeen(List<String> ids, Date seen) {
        final List<ObjectId> objectIds = new LinkedList<ObjectId>();
        for (final String id : ids) {
            objectIds.add(new ObjectId(id));
        }
        final DBObject match = new BasicDBObject("_id", new BasicDBObject("$in", objectIds));
        final DBObject set = new BasicDBObject("$set", new BasicDBObject("lastOccurred", seen));

        try {
            getJacksonCollection().updateMulti(match, set);
        } catch (final UnknownHostException e) {
            logger.log(Level.WARNING, "Failed connecting to MongoDB when updating FailureCauses' last occurrence", e);
        } catch (final AuthenticationException e) {
            logger.log(Level.WARNING, "Failed authentication when updating FailureCauses' last occurrence", e);
        }
    }

    /**
     * Generates a DBObject used for matching data as part of a MongoDb
     * aggregation query.
     *
     * @param filter the filter to create match fields for
     * @return DBObject containing fields to match
     */
    private static DBObject generateMatchFieldsBase(GraphFilterBuilder filter) {
        final DBObject matchFields = new BasicDBObject();
        if (filter != null) {
            putNonNullStringValue(matchFields, "master", filter.getMasterName());
            putNonNullStringValue(matchFields, "slaveHostName", filter.getSlaveName());
            putNonNullStringValue(matchFields, "projectName", filter.getProjectName());
            putNonNullStringValue(matchFields, "result", filter.getResult());

            putNonNullBasicDBObject(matchFields, "buildNumber", "$in", filter.getBuildNumbers());
            putNonNullBasicDBObject(matchFields, "startingTime", "$gte", filter.getSince());
            putNonNullBasicDBObject(matchFields, "result", "$ne", filter.getExcludeResult());
        }
        return matchFields;
    }

    /**
     * Generates the standard DBObject for filtering, with the additional exclusion of successful builds.
     *
     * @param filter the filter to create match fields for
     * @return DBObject containing fields to match
     */
    private static DBObject generateMatchFields(GraphFilterBuilder filter) {
        final DBObject matchFields = generateMatchFieldsBase(filter);
        putNonNullBasicDBObject(matchFields, "result", "$ne", "SUCCESS");

        return matchFields;
    }

    /**
     * Puts argument value to the dbObject if the value is non-null.
     * @param dbObject object to put value to.
     * @param key the key to map the value to.
     * @param value the value to set.
     */
    private static void putNonNullStringValue(DBObject dbObject, String key, String value) {
        if (value != null) {
            dbObject.put(key, value);
        }
    }

    /**
     * Puts argument value to the dbObject if the value is non-null.
     * The value will be added with an MongoDB operator, for example "$in" or "$gte".
     * @param dbObject object to put value to.
     * @param key the key to map the value to.
     * @param operator the MongoDB operator to add together with the value.
     * @param value the value to set.
     */
    private static void putNonNullBasicDBObject(DBObject dbObject, String key,
            String operator, Object value) {
        if (value != null) {
            dbObject.put(key, new BasicDBObject(operator, value));
        }
    }

    @Override
    public List<ObjectCountPair<FailureCause>> getNbrOfFailureCauses(GraphFilterBuilder filter) {

        final List<ObjectCountPair<String>> nbrOfFailureCausesPerId = getNbrOfFailureCausesPerId(filter, 0);
        final List<ObjectCountPair<FailureCause>> nbrOfFailureCauses = new ArrayList<ObjectCountPair<FailureCause>>();
        try {
            for (final ObjectCountPair<String> countPair : nbrOfFailureCausesPerId) {
                final String id = countPair.getObject();
                final int count = countPair.getCount();
                final FailureCause failureCause = getCause(id);
                if (failureCause != null) {
                    nbrOfFailureCauses.add(new ObjectCountPair<FailureCause>(failureCause, count));
                }
            }
        } catch (final Exception e) {
            logger.fine("Unable to count failure causes");
            e.printStackTrace();
        }
        return nbrOfFailureCauses;
    }

    @Override
    public List<ObjectCountPair<String>> getFailureCauseNames(GraphFilterBuilder filter) {
        final List<ObjectCountPair<String>> nbrOfFailureCauseNames = new ArrayList<ObjectCountPair<String>>();
        for (final ObjectCountPair<FailureCause> countPair : getNbrOfFailureCauses(filter)) {
            final FailureCause failureCause = countPair.getObject();
            if (failureCause.getName() != null) {
                nbrOfFailureCauseNames.add(new ObjectCountPair<String>(failureCause.getName(), countPair.getCount()));
            }
        }
        return nbrOfFailureCauseNames;
    }

    @Override
    public Map<Integer, List<FailureCause>> getFailureCausesPerBuild(GraphFilterBuilder filter) {
        final Map<Integer, List<FailureCause>> nbrOfFailureCausesPerBuild = new HashMap<Integer, List<FailureCause>>();
        final DBObject matchFields = generateMatchFields(filter);
        final DBObject match = new BasicDBObject("$match", matchFields);

        final DBObject unwind = new BasicDBObject("$unwind", "$failureCauses");

        final DBObject groupFields = new BasicDBObject("_id", "$buildNumber");
        groupFields.put("failureCauses", new BasicDBObject("$addToSet", "$failureCauses.failureCause"));
        final DBObject group = new BasicDBObject("$group", groupFields);

        final DBObject sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1));

        AggregationOutput output;
        try {
            output = getStatisticsCollection().aggregate(match, unwind, group, sort);
            for (final DBObject result : output.results()) {
                final List<FailureCause> failureCauses = new ArrayList<FailureCause>();
                final Integer buildNumber = (Integer)result.get("_id");
                final BasicDBList failureCauseRefs = (BasicDBList)result.get("failureCauses");
                for (final Object o : failureCauseRefs) {
                    final DBRef failureRef = (DBRef)o;
                    final String id = failureRef.getId().toString();
                    final FailureCause failureCause = getCause(id);
                    failureCauses.add(failureCause);
                }

                nbrOfFailureCausesPerBuild.put(buildNumber, failureCauses);
            }
        } catch (final Exception e) {
            logger.fine("Unable to count failure causes by build");
            e.printStackTrace();
        }

        return nbrOfFailureCausesPerBuild;
    }

    /**
     * Generates a {@link DBObject} used for grouping data into time intervals
     * @param intervalSize the interval size, should be set to Calendar.HOUR_OF_DAY,
     * Calendar.DATE or Calendar.MONTH.
     * @return DBObject to be used for time grouping
     */
    private DBObject generateTimeGrouping(int intervalSize) {
        final DBObject timeFields = new BasicDBObject();
        if (intervalSize == Calendar.HOUR_OF_DAY) {
            timeFields.put("hour", new BasicDBObject("$hour", "$startingTime"));
        }
        if (intervalSize == Calendar.HOUR_OF_DAY || intervalSize == Calendar.DATE) {
            timeFields.put("dayOfMonth", new BasicDBObject("$dayOfMonth", "$startingTime"));
        }
        timeFields.put("month", new BasicDBObject("$month", "$startingTime"));
        timeFields.put("year", new BasicDBObject("$year", "$startingTime"));
        return timeFields;
    }

    /**
     * Generates a {@link TimePeriod} based on a MongoDB grouping aggregation result.
     * @param result the result to interpret
     * @param intervalSize the interval size, should be set to Calendar.HOUR_OF_DAY,
     * Calendar.DATE or Calendar.MONTH.
     * @return TimePeriod
     */
    private TimePeriod generateTimePeriodFromResult(DBObject result, int intervalSize) {
        final BasicDBObject groupedAttrs = (BasicDBObject)result.get("_id");
        final int month = groupedAttrs.getInt("month");
        final int year = groupedAttrs.getInt("year");

        final Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        // MongoDB timezone is UTC:
        c.setTimeZone(new SimpleTimeZone(0, "UTC"));

        TimePeriod period;
        if (intervalSize == Calendar.HOUR_OF_DAY) {
            final int dayOfMonth = groupedAttrs.getInt("dayOfMonth");
            c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
            final int hour = groupedAttrs.getInt("hour");
            c.set(Calendar.HOUR_OF_DAY, hour);

            period = new Hour(c.getTime());
        } else if (intervalSize == Calendar.DATE) {
            final int dayOfMonth = groupedAttrs.getInt("dayOfMonth");
            c.set(Calendar.DAY_OF_MONTH, dayOfMonth);

            period = new Day(c.getTime());
        } else {
            period = new Month(c.getTime());
        }
        return period;
    }

    @Override
    public List<FailureCauseTimeInterval> getFailureCausesPerTime(int intervalSize, GraphFilterBuilder filter,
            boolean byCategories) {
        final List<FailureCauseTimeInterval> failureCauseIntervals = new ArrayList<FailureCauseTimeInterval>();
        final Map<MultiKey, FailureCauseTimeInterval> categoryTable = new HashMap<MultiKey, FailureCauseTimeInterval>();

        final DBObject matchFields = generateMatchFields(filter);
        final DBObject match = new BasicDBObject("$match", matchFields);

        final DBObject unwind = new BasicDBObject("$unwind", "$failureCauses");

        final DBObject idFields = generateTimeGrouping(intervalSize);
        idFields.put("failureCause", "$failureCauses.failureCause");
        final DBObject groupFields = new BasicDBObject();
        groupFields.put("_id", idFields);
        groupFields.put("number", new BasicDBObject("$sum", 1));
        final DBObject group = new BasicDBObject("$group", groupFields);

        AggregationOutput output;
        try {
            output = getStatisticsCollection().aggregate(match, unwind, group);
            for (final DBObject result : output.results()) {
                final int number = (Integer)result.get("number");

                final TimePeriod period = generateTimePeriodFromResult(result, intervalSize);

                final BasicDBObject groupedAttrs = (BasicDBObject)result.get("_id");
                final DBRef failureRef = (DBRef)groupedAttrs.get("failureCause");
                final String id = failureRef.getId().toString();
                final FailureCause failureCause = getCause(id);

                if (byCategories) {
                    if (failureCause.getCategories() != null) {
                        for (final String category : failureCause.getCategories()) {
                            final MultiKey multiKey = new MultiKey(category, period);
                            FailureCauseTimeInterval interval = categoryTable.get(multiKey);
                            if (interval == null) {
                                interval = new FailureCauseTimeInterval(period, category, number);
                                categoryTable.put(multiKey, interval);
                                failureCauseIntervals.add(interval);
                            } else {
                                interval.addNumber(number);
                            }
                        }
                    }
                } else {
                    final FailureCauseTimeInterval timeInterval = new FailureCauseTimeInterval(period, failureCause.getName(),
                            failureCause.getId(), number);
                    failureCauseIntervals.add(timeInterval);
                }
            }
        } catch (final UnknownHostException e) {
            logger.fine("Unable to get failure causes by time");
            e.printStackTrace();
        } catch (final AuthenticationException e) {
            logger.fine("Unable to get failure causes by time");
            e.printStackTrace();
        }

        return failureCauseIntervals;
    }

    @Override
    public List<ObjectCountPair<String>> getNbrOfFailureCategoriesPerName(GraphFilterBuilder filter, int limit) {

        final List<ObjectCountPair<String>> nbrOfFailureCausesPerId = getNbrOfFailureCausesPerId(filter, 0);
        final Map<String, Integer> nbrOfFailureCategoriesPerName = new HashMap<String, Integer>();

        for (final ObjectCountPair<String> countPair : nbrOfFailureCausesPerId) {
            final String id = countPair.getObject();
            final int count = countPair.getCount();
            FailureCause failureCause = null;
            try {
                failureCause = getCause(id);
            } catch (final Exception e) {
                logger.fine("Unable to count failure causes by name");
                e.printStackTrace();
            }
            if (failureCause != null) {
                if (failureCause.getCategories() == null) {
                    Integer currentNbr = nbrOfFailureCategoriesPerName.get(null);
                    if (currentNbr == null) {
                        currentNbr = 0;
                    }
                    currentNbr += count;
                    nbrOfFailureCategoriesPerName.put(null, currentNbr);
                } else {
                    for (final String category : failureCause.getCategories()) {
                        Integer currentNbr = nbrOfFailureCategoriesPerName.get(category);
                        if (currentNbr == null) {
                            currentNbr = 0;
                        }
                        currentNbr += count;
                        nbrOfFailureCategoriesPerName.put(category, currentNbr);
                    }
                }
            }
        }
        List<ObjectCountPair<String>> countList = new ArrayList<ObjectCountPair<String>>();
        for (final Entry<String, Integer> entry : nbrOfFailureCategoriesPerName.entrySet()) {
            final String name = entry.getKey();
            final int count = entry.getValue();
            countList.add(new ObjectCountPair<String>(name, count));
        }
        Collections.sort(countList, ObjectCountPair.countComparator());
        if (limit > 0 && countList.size() > limit) {
            countList = countList.subList(0, limit);
        }

        return countList;
    }

    @Override
    public void removeBuildfailurecause(Run build) throws Exception {
        final BasicDBObject searchObj = new BasicDBObject();
        searchObj.put("projectName", build.getParent().getFullName());
        searchObj.put("buildNumber", build.getNumber());
        searchObj.put("master", BfaUtils.getMasterName());
        final com.mongodb.DBCursor dbcursor = getStatisticsCollection().find(searchObj);
        if (dbcursor != null && dbcursor.size() > 0) {
            while (dbcursor.hasNext()) {
                getStatisticsCollection().remove(dbcursor.next());
                logger.log(Level.INFO, build.getDisplayName() + " build failure cause removed");
            }
        } else {
            logger.log(Level.INFO, build.getDisplayName() + " build failure cause "
                    + "value is null or initial scanning ");
        }
    }

    /**
     * Adds the FailureCauses from the list to the DBObject.
     * @param object the DBObject to add to.
     * @param failureCauseStatisticsList the list of FailureCauseStatistics to add.
     * @throws UnknownHostException If the mongoDB host cannot be found.
     * @throws AuthenticationException if the mongoDB authentication fails.
     */
    private void addFailureCausesToDBObject(DBObject object, List<FailureCauseStatistics> failureCauseStatisticsList)
            throws UnknownHostException, AuthenticationException {
        if (failureCauseStatisticsList != null && !failureCauseStatisticsList.isEmpty()) {
            final List<DBObject> failureCauseStatisticsObjects = new LinkedList<DBObject>();

            for (final FailureCauseStatistics failureCauseStatistics : failureCauseStatisticsList) {
                final DBObject failureCauseStatisticsObject = new BasicDBObject();
                final ObjectId id = new ObjectId(failureCauseStatistics.getId());
                final DBRef failureCauseRef = new DBRef(getDb(), COLLECTION_NAME, id);
                failureCauseStatisticsObject.put("failureCause", failureCauseRef);
                final List<FoundIndication> foundIndicationList = failureCauseStatistics.getIndications();
                addIndicationsToDBObject(failureCauseStatisticsObject, foundIndicationList);
                failureCauseStatisticsObjects.add(failureCauseStatisticsObject);
            }
            object.put("failureCauses", failureCauseStatisticsObjects);
        }
    }

    /**
     * Adds the indications from the list to the DBObject.
     * @param object the DBObject to add to.
     * @param indications the list of indications to add.
     */
    private void addIndicationsToDBObject(DBObject object, List<FoundIndication> indications) {
        if (indications != null && !indications.isEmpty()) {
            final List<DBObject> foundIndicationObjects = new LinkedList<DBObject>();
            for (final FoundIndication foundIndication : indications) {
                final DBObject foundIndicationObject = new BasicDBObject();
                foundIndicationObject.put("pattern", foundIndication.getPattern());
                foundIndicationObject.put("matchingFile", foundIndication.getMatchingFile());
                foundIndicationObject.put("matchingString", foundIndication.getMatchingString());
                foundIndicationObjects.add(foundIndicationObject);
            }
            object.put("indications", foundIndicationObjects);
        }
    }

    @Override
    public Descriptor<KnowledgeBase> getDescriptor() {
        return Jenkins.getInstance().getDescriptorByType(MongoDBKnowledgeBaseDescriptor.class);
    }

    /**
     * Gets the connection to the MongoDB
     * @return the Mongo.
     * @throws UnknownHostException if the host cannot be found.
     */
    private Mongo getMongoConnection() throws UnknownHostException {
        if (mongo == null) {
            mongo = new Mongo(host, port);
        }
        return mongo;
    }

    /**
     * Gets the DB.
     * @return The DB.
     * @throws UnknownHostException if the host cannot be found.
     * @throws AuthenticationException if we cannot authenticate towards the database.
     */
    private DB getDb() throws UnknownHostException, AuthenticationException {
        if (db == null) {
            db = getMongoConnection().getDB(dbName);
        }
        if (Util.fixEmpty(userName) != null && Util.fixEmpty(Secret.toString(password)) != null) {
            final char[] pwd = password.getPlainText().toCharArray();
            if (!db.authenticate(userName, pwd)) {
                throw new AuthenticationException("Could not athenticate with the mongo database");
            }
        }
        return db;
    }

    /**
     * Gets the DBCollection.
     * @return The db collection.
     * @throws UnknownHostException if the host cannot be found.
     * @throws AuthenticationException if we cannot authenticate towards the database.
     */
    private DBCollection getCollection() throws UnknownHostException, AuthenticationException {
        if (collection == null) {
            collection = getDb().getCollection(COLLECTION_NAME);
        }
        return collection;
    }

    /**
     * Gets the Statistics DBCollection.
     * @return The statistics db collection.
     * @throws UnknownHostException if the host cannot be found.
     * @throws AuthenticationException if we cannot authenticate towards the database.
     */
    private synchronized DBCollection getStatisticsCollection() throws UnknownHostException, AuthenticationException {
        if (statisticsCollection == null) {
            statisticsCollection = getDb().getCollection(STATISTICS_COLLECTION_NAME);
        }
        return statisticsCollection;
    }

    /**
     * Gets the JacksonDBCollection for FailureCauses.
     * @return The jackson db collection.
     * @throws UnknownHostException if the host cannot be found.
     * @throws AuthenticationException if we cannot authenticate towards the database.
     */
    private synchronized JacksonDBCollection<FailureCause, String> getJacksonCollection()
            throws UnknownHostException, AuthenticationException {
        if (jacksonCollection == null) {
            if (collection == null) {
                collection = getCollection();
            }
            jacksonCollection = JacksonDBCollection.wrap(collection, FailureCause.class, String.class);
        }
        return jacksonCollection;
    }

    /**
     * Gets the JacksonDBCollection for Statistics.
     * @return The jackson db collection.
     * @throws UnknownHostException if the host cannot be found.
     * @throws AuthenticationException if we cannot authenticate towards the database.
     */
    private synchronized JacksonDBCollection<Statistics, String> getJacksonStatisticsCollection()
            throws UnknownHostException, AuthenticationException {
        if (jacksonStatisticsCollection == null) {
            if (statisticsCollection == null) {
                statisticsCollection = getStatisticsCollection();
            }
            jacksonStatisticsCollection = JacksonDBCollection.wrap(statisticsCollection, Statistics.class, String.class);
        }
        return jacksonStatisticsCollection;
    }

    /**
     * Descriptor for {@link MongoDBKnowledgeBase}.
     */
    @Extension
    public static class MongoDBKnowledgeBaseDescriptor extends KnowledgeBaseDescriptor {

        @Override
        public String getDisplayName() {
            return Messages.MongoDBKnowledgeBase_DisplayName();
        }

        /**
         * Convenience method for jelly.
         * @return the default port.
         */
        public int getDefaultPort() {
            return MONGO_DEFAULT_PORT;
        }

        /**
         * Checks that the host name is not empty.
         *
         * @param value the pattern to check.
         * @return {@link hudson.util.FormValidation#ok()} if everything is well.
         */
        public FormValidation doCheckHost(@QueryParameter("value") final String value) {
            if (Util.fixEmpty(value) == null) {
                return FormValidation.error("Please provide a host name!");
            } else {
                final Matcher m = Pattern.compile("\\s").matcher(value);
                if (m.find()) {
                    return FormValidation.error("Host name contains white space!");
                }
                return FormValidation.ok();
            }
        }

        /**
         * Checks that the port number is not empty and is a number.
         *
         * @param value the port number to check.
         * @return {@link hudson.util.FormValidation#ok()} if everything is well.
         */
        public FormValidation doCheckPort(@QueryParameter("value") String value) {
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
         * @param value the database name to check.
         * @return {@link hudson.util.FormValidation#ok()} if everything is well.
         */
        public FormValidation doCheckDBName(@QueryParameter("value") String value) {
            if (value == null || value.isEmpty()) {
                return FormValidation.error("Please provide a database name!");
            } else {
                final Matcher m = Pattern.compile("\\s").matcher(value);
                if (m.find()) {
                    return FormValidation.error("Database name contains white space!");
                }
                return FormValidation.ok();
            }
        }

        /**
         * Tests if the provided parameters can connect to the Mongo database.
         * @param host the host name.
         * @param port the port.
         * @param dbName the database name.
         * @param userName the user name.
         * @param password the password.
         * @return {@link FormValidation#ok() } if can be done,
         *         {@link FormValidation#error(java.lang.String) } otherwise.
         */
        public FormValidation doTestConnection(
                @QueryParameter("host") final String host,
                @QueryParameter("port") final int port,
                @QueryParameter("dbName") final String dbName,
                @QueryParameter("userName") final String userName,
                @QueryParameter("password") final String password) {
            final MongoDBKnowledgeBase base = new MongoDBKnowledgeBase(host, port, dbName, userName,
                    Secret.fromString(password), false, false);
            try {
                base.getCollection();
            } catch (final Exception e) {
                return FormValidation.error(e, Messages.MongoDBKnowledgeBase_ConnectionError());
            }
            return FormValidation.ok(Messages.MongoDBKnowledgeBase_ConnectionOK());
        }
    }
}
