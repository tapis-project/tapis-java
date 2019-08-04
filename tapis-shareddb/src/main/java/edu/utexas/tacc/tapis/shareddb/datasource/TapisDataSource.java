package edu.utexas.tacc.tapis.shareddb.datasource;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.p6spy.engine.spy.P6DataSource;
import com.zaxxer.hikari.HikariDataSource;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator.MetricReporterType;

/** This class provides a simple way to share a datasource with all threads in
 * an application.  A datasource is created once and then reused on all
 * subsequent getDataSource() calls until close() is called.  The main use of
 * the datasource is to retrieve a connection from the connection pool.
 * 
 * @author rcardone
 */
public final class TapisDataSource 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(TapisDataSource.class);
    
    // Set the minimum postgres server version.
    public static final String DFT_MIN_SERVER_VERSION = "11.4";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The single datasource used by this service.
    private static P6DataSource _ds;           // log enabled data source
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getDataSource:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Create a datasource configured for this service.
     * 
     * @param appName - name of the application to be used on all connections
     * @param poolName - a user chosen name for this pool, preferably unique
     * @param jdbcUrl - the jdbc url, such as "jdbc:mysql://localhost:3306/agave-api"
     * @param user - the database user's name 
     * @param password - the database user's password
     * @param maxPoolSize - the size of the connection pool
     * @param meterMinutes - the metering interval
     * 
     * @return the default data source.
     * @throws AloeException 
     */
    public static synchronized DataSource getDataSource(String appName,
                                                        String poolName, 
    		                                            String jdbcUrl, 
                                                        String user, 
                                                        String password, 
                                                        int    maxPoolSize,
                                                        int    meterMinutes) 
      throws TapisException
    {
    	// Create and configure the datasource if necessary.
    	if (_ds == null) 
    	{
    		try {
    			// Generate a data source.
    			HikariDSGenerator dsgen = new HikariDSGenerator();
    			HikariDataSource hikariDS = dsgen.getDataSource(appName, poolName, jdbcUrl, 
    				                                            user, password, maxPoolSize);
    			
    			// Customize connections.
    			dsgen.setReliabilityOptions(hikariDS);
    			dsgen.setMetricRegistry(hikariDS, meterMinutes, MetricReporterType.SL4J);
        
    			// Assign as the service's only datasource.
    			_ds = new P6DataSource(hikariDS);
    		} 
    		catch (Exception e) {
    		  String msg = MsgUtils.getMsg("DB_FAILED_DATASOURCE_CREATE", poolName, maxPoolSize, user, jdbcUrl);
    			_log.error(msg, e);
    			throw new TapisException(msg, e);
    		}
    	}
    	
    	return _ds;
    }
	
    /* ---------------------------------------------------------------------- */
    /* getDataSource:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Returns the value currently assigned to the static datasource field.  
     * If null is returned, the caller should call the overloaded version of 
     * this method that will initialize a datasource.
     * 
     * @return null or a datasource reference
     */
    public static synchronized DataSource getDataSource()
    {
    	return _ds;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* setAssumeMinServerVersion:                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Optional performance setting that should be called before connections are
     * established.
     * 
     * @param version
     */
    public void setAssumeMinServerVersion(String version)
    {
        getHikariDataSource().addDataSourceProperty("assumeMinServerVersion", version);
    }
    
    /* ---------------------------------------------------------------------- */
    /* close:                                                                 */
    /* ---------------------------------------------------------------------- */
    /** Close the datasource and reset the datasource field back to null. */
    public static synchronized void close()
    {
    	if (_ds != null) {
    	    getHikariDataSource().close();
    		_ds = null;
    	}
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getHikariDataSource:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Get the underlying HikariDataSource wrapped by the p6spy datasource.
     * Since we don't need to go directly to the real Hikari datasource much
     * it's ok to call unwrap() with its reflective code.  If we need to bypass
     * the wrapper and go to Hikari often, we should consider caching the 
     * Hikari datasource ourselves. 
     * 
     * @return the real datasource
     * @throws TapisRuntimeException on error
     */
    private static HikariDataSource getHikariDataSource()
     throws TapisRuntimeException
    {
        // This should never fail on an initialized datasource.
        try {return _ds.unwrap(HikariDataSource.class);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("DB_UNWRAP_DATASOURCE_ERROR", e.getMessage());
                _log.error(msg, e);
                throw new TapisRuntimeException(msg, e);
            }
    }
}
