package com.gmail.eksuzyan.pavel.concurrency.tasks;

import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.stores.Slave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NavigableSet;

/**
 * @author Pavel Eksuzian.
 *         Created: 19.03.2017.
 */
public class SendingTask implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(SendingTask.class);

    private final Slave slave;
    private final Request request;
    private final NavigableSet<Request> failedRequests;

    public SendingTask(Slave slave, Request request, NavigableSet<Request> failedRequests) {
        this.slave = slave;
        this.request = request;
        this.failedRequests = failedRequests;
    }

    @Override
    public void run() {
        try {
            slave.postProject(
                    request.getProject().getId(),
                    request.getProject().getVersion(),
                    request.getProject().getData());

            LOG.debug("[+] => Slave #{} => {}.", request.getSlaveId(), request);
        } catch (Throwable e) {
            if (failedRequests.add(request))
                LOG.debug("[+] => failedRequests(size={}) => {}", failedRequests.size(), request);
            else
                LOG.debug("[-] => failedRequests(size={}) => {}", failedRequests.size(), request);

            LOG.debug("[-] => Slave #{} => {}.", request.getSlaveId(), request);
        }
    }
}
