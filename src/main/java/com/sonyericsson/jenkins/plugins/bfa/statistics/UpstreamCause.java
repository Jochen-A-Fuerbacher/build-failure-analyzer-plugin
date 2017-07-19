package com.sonyericsson.jenkins.plugins.bfa.statistics;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import hudson.model.Cause;

/**
 * Upstream cause.
 */
@Embeddable
public class UpstreamCause {
	@Column(name="UPSTREAMPROJECT")
    private String project;
	@Column(name="UPSTREAMBUILD")
    private int build;

    /**
     * JSON constructor.
     *
     * @param project Upstream build project.
     * @param build Upstream build number.
     */
    @JsonCreator
    public UpstreamCause(@JsonProperty("project") String project, @JsonProperty("build") int build) {
        this.project = project;
        this.build = build;
    }

    /**
     * Constructor for Cause.UpstreamCause.
     * @param upstreamCause The Cause to copy.
     */
    public UpstreamCause(Cause.UpstreamCause upstreamCause) {
        if (upstreamCause == null) {
            this.project = "";
            this.build = 0;
        } else {
            this.project = upstreamCause.getUpstreamProject();
            this.build = upstreamCause.getUpstreamBuild();
        }
    }

    /**
	 * Default constructor. <strong>Do not use this unless you are a serializer.</strong>
	 */
    public UpstreamCause() {
    }

    /**
     * Getter for the upstream build project.
     * @return the upstream build project.
     */
    public String getUpstreamProject() {
        return project;
    }

    /**
     * Setter for the upstream build project.
     * @param p The project to set.
     */
    public void setUpstreamProject(String p) {
        this.project = p;
    }

    /**
     * Getter for the upstream build number.
     * @return the upstream build number.
     */
    public int getUpstreamBuild() {
        return build;
    }

    /**
     * Setter for the upstream build number.
     * @param buildNr The project build to set.
     */
    public void setUpstreamBuild(int buildNr) {
        this.build = buildNr;
    }
}