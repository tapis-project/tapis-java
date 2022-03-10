package edu.utexas.tacc.tapis.security.authz.impl;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkShareDao;
import edu.utexas.tacc.tapis.security.authz.dao.SkShareDao.ShareFilter;
import edu.utexas.tacc.tapis.security.authz.model.SkShare;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

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
     * @throws TapisImplException 
     */
    public int shareResource(SkShare skshare) throws TapisImplException
    {
        // Get the dao.
        SkShareDao dao = null;
        try {dao = getSkShareDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "share");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        int rows = 0;
        try {rows = dao.shareResource(skshare);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_SHARE_DB_INSERT_ERROR", skshare.dumpContent());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return rows;
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
     * @throws TapisImplException 
     */
    public List<SkShare> getShares(Map<ShareFilter, Object> filter) throws TapisImplException
    {
        // Get the dao.
        SkShareDao dao = null;
        try {dao = getSkShareDao();}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_DAO_ERROR", "share");
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.INTERNAL_SERVER_ERROR);
            }
        
        // Create the role.
        List<SkShare> list = null;
        try {list = dao.getShares(filter);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_SHARE_DB_SELECT_ERROR", 
                                             filter.get(ShareFilter.TENANT));
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return list;
    }
}
