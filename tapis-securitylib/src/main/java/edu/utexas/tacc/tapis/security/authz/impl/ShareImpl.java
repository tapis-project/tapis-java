package edu.utexas.tacc.tapis.security.authz.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.model.SkShare;

public class ShareImpl
  extends BaseImpl
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ShareImpl.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singleton instance of this class.
    private static ShareImpl _instance;
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    private ShareImpl() {}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    public static ShareImpl getInstance()
    {
        // Create the singleton instance if necessary.
        if (_instance == null) {
            synchronized (ShareImpl.class) {
                if (_instance == null) _instance = new ShareImpl();
            }
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* shareResource:                                                         */
    /* ---------------------------------------------------------------------- */
    /** This method insert a new share into the share table if it doesn't already
     * exist.  It also updates the id field and created fields with values from
     * the database.
     * 
     * 1 is returned if a new row was inserted, otherwise 0 is returned.
     * 
     * @param skshare a new share object
     * @return the number of rows inserted (0 or 1) and an updated share object
     */
    public int shareResource(SkShare skshare)
    {
        return 0;
    }
}
