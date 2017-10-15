package com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.logic.slave.DefaultSlave;

/**
 * @author Pavel Eksuzian.
 *         Created: 22.03.2017.
 */
public class PendingSlave extends DefaultSlave {

    public PendingSlave() {
        this(null);
    }

    public PendingSlave(String name) {
        super(name);
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        Thread.sleep((long) (Math.random() * 10000));
        super.postProject(projectId, version, data);
    }
}
