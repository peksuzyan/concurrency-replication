package com.gmail.eksuzyan.pavel.concurrency.stores;

/**
 * @author Pavel Eksuzian.
 *         Created: 22.03.2017.
 */
public class PendingSlave extends Slave {

    public void postProject(String projectId, long version, String data) throws Exception {
        Thread.sleep((long) (Math.random() * 10000));
        super.postProject(projectId, version, data);
    }

}
