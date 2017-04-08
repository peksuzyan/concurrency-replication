package com.gmail.eksuzyan.pavel.concurrency.entities;

/**
 * @author Pavel Eksuzian.
 *         Created: 06.04.2017.
 */
public class Message {

    public final String projectId;
    public final String data;

    public Message(String projectId, String data) {
        this.projectId = projectId;
        this.data = data;
    }

}
