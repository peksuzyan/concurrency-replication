package com.gmail.eksuzyan.pavel.concurrency.logic.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.logic.slave.DefaultSlave;

/**
 * @author Pavel Eksuzian.
 *         Created: 09.07.2017.
 */
public class AlwaysThrowingSlave extends DefaultSlave {

    public AlwaysThrowingSlave() {
        this(null);
    }

    public AlwaysThrowingSlave(String name) {
        super(name);
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        throw new Exception();
    }
}
