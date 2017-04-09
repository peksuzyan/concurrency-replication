package com.gmail.eksuzyan.pavel.concurrency.tasks;

import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;

/**
 * @author Pavel Eksuzian.
 *         Created: 19.03.2017.
 */
public class SendingTask implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(SendingTask.class);

    private final Slave slave;
    private final Request request;
    private final BlockingQueue<Request> failedRequests;

    public SendingTask(Slave slave, Request request, BlockingQueue<Request> failedRequests) {
        this.slave = slave;
        this.request = request;
        this.failedRequests = failedRequests;
    }

    @Override
    public void run() {
        try {
            slave.postProject(
                    request.project.id,
                    request.project.version,
                    request.project.data);

            LOG.debug("[+] => {} => {}.", request.slave, request);
        } catch (Throwable e) {
            LOG.debug("[-] => {} => {}.", request.slave, request);

            try {
                failedRequests.put(request.setCodeAndIncAttempt(1));
                LOG.debug("[+] => failedRequests => {}", request);
            } catch (InterruptedException ex) {
                LOG.error("Request has been lost due to unknown error.", ex);
            }
        }
    }
}
