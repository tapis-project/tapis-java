package edu.utexas.tacc.tapis.sample.model;

import java.time.Instant;

public final class Sample 
{
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private long            id;                    // Unique database sequence number
    private String          text;                  // Human-readable text
    private Instant         updated;               // Time sample was last updated

    /* ********************************************************************** */
    /*                                Accessors                               */
    /* ********************************************************************** */
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public String getText() {
        return text;
    }
    public void setText(String text) {
        this.text = text;
    }
    public Instant getUpdated() {
        return updated;
    }
    public void setUpdated(Instant updated) {
        this.updated = updated;
    }
}
