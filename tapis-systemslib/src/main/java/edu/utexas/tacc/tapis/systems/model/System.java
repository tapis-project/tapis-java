package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;

public final class System
{
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private long            id;                    // Unique database sequence number
    private String name;                  // Name of the system
    private Instant         updated;               // Time system was last updated

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
        return name;
    }
    public void setText(String name) {
        this.name = name;
    }
    public Instant getUpdated() {
        return updated;
    }
    public void setUpdated(Instant updated) {
        this.updated = updated;
    }
}
