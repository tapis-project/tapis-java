package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;

public abstract class AbstractDao 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(AbstractDao.class);
  
  /* ---------------------------------------------------------------------- */
  /* getConnection:                                                         */
  /* ---------------------------------------------------------------------- */
  /** Return a connection from the static datasource.  Create the datasource
   * on demand if it doesn't exist.
   * 
   * @return a database connection
   * @throws TapisException on error
   */
  protected static synchronized Connection getConnection() 
   throws TapisException
  {
    // Use the existing datasource. 
    DataSource ds = getDataSource();
    
    // Get the connection.
    Connection conn = null;
    try {conn = ds.getConnection();}
      catch (Exception e) {
        String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION");
        _log.error(msg, e);
        throw new TapisDBConnectionException(msg, e);
      }
    
    return conn;
  }

  /* ---------------------------------------------------------------------- */
  /* getDataSource:                                                         */
  /* ---------------------------------------------------------------------- */
  protected static DataSource getDataSource() 
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
}
