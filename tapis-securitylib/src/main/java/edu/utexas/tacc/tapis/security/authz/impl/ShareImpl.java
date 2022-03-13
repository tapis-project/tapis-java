package edu.utexas.tacc.tapis.security.authz.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.SkShareDao;
import edu.utexas.tacc.tapis.security.authz.model.SkShare;
import edu.utexas.tacc.tapis.security.authz.model.SkShareInputFilter;
import edu.utexas.tacc.tapis.security.authz.model.SkSharePrivilegeSelector;
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
     * @param skShare a new share object
     * @return the number of rows inserted (0 or 1) and an updated share object
     * @throws TapisImplException 
     */
    public int shareResource(SkShare skShare) throws TapisImplException
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
        try {rows = dao.shareResource(skShare);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_SHARE_DB_INSERT_ERROR", skShare.dumpContent());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return rows;
    }

    /* ---------------------------------------------------------------------- */
    /* getShare:                                                              */
    /* ---------------------------------------------------------------------- */
    /** This method retrieves a single shared resource object by ID.  If no
     * record exists with that ID, null is returned.
     * 
     * @param tenant the obo tenant
     * @param id the id of the share
     * @return the share object or null
     * @throws TapisImplException 
     */
    public SkShare getShare(String tenant, int id) throws TapisImplException
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
        SkShare skShare = null;
        try {skShare = dao.getShare(tenant, id);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_SHARE_DB_SELECT_ERROR", tenant);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return skShare;
    }

    /* ---------------------------------------------------------------------- */
    /* getShares:                                                             */
    /* ---------------------------------------------------------------------- */
    /** This method retrieve zero or more shared resource objects depending on
     * the filter values.  
     * 
     * @param filter the search criteria
     * @return a non-null list of shares
     * @throws TapisImplException 
     */
    public List<SkShare> getShares(SkShareInputFilter filter) throws TapisImplException
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
                String msg = MsgUtils.getMsg("SK_SHARE_DB_SELECT_ERROR", filter.getTenant());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return list;
    }

    /* ---------------------------------------------------------------------- */
    /* deleteShare:                                                           */
    /* ---------------------------------------------------------------------- */
    /** This method deletes a single shared resource object by ID.  If no
     * record exists with that ID, null is returned.
     * 
     * @param tenant the obo tenant
     * @param id the id of the share
     * @return the share object or null
     * @throws TapisImplException 
     */
    public int deleteShare(String tenant, int id) throws TapisImplException
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
        try {rows = dao.deleteShare(tenant, id);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_SHARE_DB_DELETE_ERROR", tenant, id);
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return rows;
    }
    
    /* ---------------------------------------------------------------------- */
    /* hasPrivilege:                                                          */
    /* ---------------------------------------------------------------------- */
    public boolean hasPrivilege(SkSharePrivilegeSelector sel) throws TapisImplException
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
        boolean hasPrivilege = false;
        try {hasPrivilege = dao.hasPrivilege(sel);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("SK_SHARE_DB_SELECT_ERROR", sel.getTenant());
                _log.error(msg, e);
                throw new TapisImplException(msg, e, Condition.BAD_REQUEST);         
            }
        
        return hasPrivilege;
    }
}
