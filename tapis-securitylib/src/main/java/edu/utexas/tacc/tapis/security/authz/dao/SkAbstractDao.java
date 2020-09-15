package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.config.RuntimeParameters;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;

public abstract class SkAbstractDao 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAbstractDao.class);
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The database datasource provided by clients.
    protected final DataSource _ds;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Initialize the datasource.
     * 
     * @param dataSource the non-null datasource 
     * @throws TapisException 
     */
    public SkAbstractDao() throws TapisException
    {
      _ds = getDataSource();
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getDataSource:                                                         */
    /* ---------------------------------------------------------------------- */
    public static DataSource getDataSource() 
     throws TapisException
    {
      // Use the existing datasource. 
      DataSource ds = TapisDataSource.getDataSource();
      if (ds == null) {
        try {
          // Get a database connection.
          RuntimeParameters parms = RuntimeParameters.getInstance();
          ds = TapisDataSource.getDataSource(parms.getInstanceName(),
                                             parms.getDbConnectionPoolName(), 
                                             parms.getJdbcURL(),
                                             parms.getDbUser(), 
                                             parms.getDbPassword(), 
                                             parms.getDbConnectionPoolSize(),
                                             parms.getDbMeterMinutes());
        }
        catch (TapisException e) {
          // Details are already logged at exception site.
          String msg = MsgUtils.getMsg("DB_FAILED_DATASOURCE");
          _log.error(msg, e);
          throw new TapisException(msg, e);
        }
      }
      
      return ds;
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getConnection:                                                         */
    /* ---------------------------------------------------------------------- */
    protected Connection getConnection()
      throws TapisException
    {
      // Get the connection.
      Connection conn = null;
      try {conn = _ds.getConnection();}
        catch (Exception e) {
          String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION");
          _log.error(msg, e);
          throw new TapisDBConnectionException(msg, e);
        }
      
      return conn;
    }
}
