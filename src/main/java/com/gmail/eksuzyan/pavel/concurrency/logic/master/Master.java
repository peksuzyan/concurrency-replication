package com.gmail.eksuzyan.pavel.concurrency.logic.master;

import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;

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

    /**
     * Returns repeat delivery mode is used by this master instance.
     *
     * @return delivery mode
     */
    Mode getDeliveryMode();

    /**
     * Describes repeat delivery modes allowed to deliver projects to slaves.
     */
    enum Mode {

        /**
         * Schedule to deliver even those rejected requests which are older then other ones.
         */
        BROADCASTING,

        /**
         * Schedule to deliver only those rejected requests which are youngest.
         */
        SELECTING
    }

}
