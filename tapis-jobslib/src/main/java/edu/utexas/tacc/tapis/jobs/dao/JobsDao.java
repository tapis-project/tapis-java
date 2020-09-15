package edu.utexas.tacc.tapis.jobs.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;

public final class JobsDao 
 extends AbstractDao
{
    /* ********************************************************************** */
	/*                               Constants                                */
	/* ********************************************************************** */
	// Tracing.
	private static final Logger _log = LoggerFactory.getLogger(JobsDao.class);
	  
    // Keep track of the last monitoring outcome.
	private static final CallSiteToggle _lastQueryDBSucceeded = new CallSiteToggle();
	  
	/* ********************************************************************** */
	/*                              Constructors                              */
	/* ********************************************************************** */
	/* ---------------------------------------------------------------------- */
	/* constructor:                                                           */
	/* ---------------------------------------------------------------------- */
	/** The superclass initializes the datasource.
	 * 
	 * @throws TapisException on database errors
	 */
	public JobsDao() throws TapisException {}
	  
	/* ********************************************************************** */
	/*                             Public Methods                             */
	/* ********************************************************************** */
	/* ---------------------------------------------------------------------- */
	/* queryDB:                                                               */
	/* ---------------------------------------------------------------------- */
	/** Probe connectivity to the specified table.  This method is called during
	 * monitoring so most errors are not logged to avoid filling up our logs.
	 * 
	 * @param tableName the tenant id
	 * @return 0 or 1 depending on whether the table is empty or not
	 * @throws TapisException on error
	 */
	public int queryDB(String tableName) 
	  throws TapisException
	{
	    // ------------------------- Check Input -------------------------
	    // Exceptions can be throw from here.
	    if (StringUtils.isBlank(tableName)) {
	        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "queryDB", "tableName");
	        _log.error(msg);
	        throw new TapisException(msg);
	    }
	      
	    // ------------------------- Call SQL ----------------------------
	    int rows = 0;
	    Connection conn = null;
	    try
	    {
	        // Get a database connection.
	        conn = getConnection();
	          
	        // Get the select command.
	        String sql = SqlStatements.SELECT_1;
	        sql = sql.replace(":table", tableName);
	        
	        // Prepare the statement and fill in the placeholders.
	        PreparedStatement pstmt = conn.prepareStatement(sql);
	                      
	        // Issue the call for the N row result set.
	        ResultSet rs = pstmt.executeQuery();
	        if (rs.next()) rows = rs.getInt(1);
	          
	        // Close the result and statement.
	        rs.close();
	        pstmt.close();
	    
	        // Commit the transaction.
	        conn.commit();
	          
	        // Toggle the last outcome flag if necessary.
	        if (_lastQueryDBSucceeded.toggleOn())
	            _log.info(MsgUtils.getMsg("DB_SELECT_ID_ERROR_CLEARED"));
	    }
	    catch (Exception e)
	    {
	        // Rollback transaction.
	        try {if (conn != null) conn.rollback();}
	            catch (Exception e1){}
	          
	        // Log the first error after a reigning success.
	        String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "tableName", tableName, e.getMessage());
	        if (_lastQueryDBSucceeded.toggleOff()) _log.error(msg, e); 
	        throw new TapisException(msg, e);
	    }
	    finally {
	        // Always return the connection back to the connection pool.
	        try {if (conn != null) conn.close();}
	          catch (Exception e){} 
	    }
	      
	    return rows;
	}
}
