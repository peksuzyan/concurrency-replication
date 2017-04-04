package com.gmail.eksuzyan.pavel.concurrency.master;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;

import java.io.Closeable;
import java.util.Collection;

/**
 * Describes methods to interact with a master instance.
 *
 * @author Pavel Eksuzian.
 *         Created: 04.04.2017.
 */
public interface Master extends Closeable {

    /**
     * Posts a project to inner store and related slaves.
     *
     * @param projectId project id
     * @param data      project data
     */
    void postProject(String projectId, String data);

    /**
     * Returns projects which are stored by master.
     *
     * @return a set of projects
     */
    Collection<Project> getProjects();

    /**
     * Returns requests which are failed while being posted.
     *
     * @return a set of requests
     */
    Collection<Request> getFailed();

    /**
     * Returns slaves which are related to this master instance.
     *
     * @return a set of slaves
     */
    Collection<Slave> getSlaves();

    /**
     * Sets master name.
     *
     * @param name master name
     */
    void setName(String name);

    /**
     * Returns master name.
     *
     * @return master name
     */
    String getName();

}
