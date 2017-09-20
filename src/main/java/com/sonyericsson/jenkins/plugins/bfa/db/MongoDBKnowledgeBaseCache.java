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

package com.sonyericsson.jenkins.plugins.bfa.db;

import static com.sonyericsson.jenkins.plugins.bfa.db.MongoDBKnowledgeBase.NOT_REMOVED_QUERY;

import java.util.LinkedList;
import java.util.List;

import com.sonyericsson.jenkins.plugins.bfa.model.FailureCause;

import net.vz.mongodb.jackson.DBCursor;
import net.vz.mongodb.jackson.JacksonDBCollection;

/**
 * Cache for the MongoDBKnowledgeBase.
 *
 * @author Tomas Westling &lt;tomas.westling@sonyericsson.com&gt;
 */
public class MongoDBKnowledgeBaseCache extends DBKnowledgeBaseCache {
	private final JacksonDBCollection<FailureCause, String> jacksonCollection;

	/**
	 * Standard constructor.
	 *
	 * @param jacksonCollection
	 *            the JacksonDBCollection, used for accessing the database.
	 */
	public MongoDBKnowledgeBaseCache(JacksonDBCollection<FailureCause, String> jacksonCollection) {
		this.jacksonCollection = jacksonCollection;
	}

	@Override
	protected List<FailureCause> updateCausesList() {
		final List<FailureCause> list = new LinkedList<FailureCause>();
		final DBCursor<FailureCause> dbCauses = jacksonCollection.find(NOT_REMOVED_QUERY);
		while (dbCauses.hasNext()) {
			list.add(dbCauses.next());
		}
		return list;
	}

	@Override
	protected List<String> updateCategories() {
		return jacksonCollection.distinct("categories");
	}
}