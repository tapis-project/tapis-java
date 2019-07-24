package edu.utexas.tacc.tapis.shareddb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;

/** Utility to create a tapis database with US English locale.  See the 
 * CreateDatabaseParameters class for default values.
 * 
 * @author rcardone
 *
 */
public final class CreateDatabase 
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(CreateDatabase.class);
    
    // Default database name.
    private static final String ADMIN_POOL    = "adminPool";
    private static final int    MAX_POOL_SIZE = 2;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Command line parameters.
    private final CreateDatabaseParameters _parms;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public CreateDatabase(CreateDatabaseParameters parms)
    {
      // Parameters cannot be null.
      if (parms == null) {
        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "CreateTenant", "parms");
        _log.error(msg);
        throw new IllegalArgumentException(msg);
      }
      _parms = parms;
    }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* main:                                                                        */
    /* ---------------------------------------------------------------------------- */
    public static void main(String[] args)
      throws Exception 
    {
        // Parse the command line parameters.
        CreateDatabaseParameters parms = null;
        try {parms = new CreateDatabaseParameters(args);}
          catch (Exception e) {
             String msg = MsgUtils.getMsg("TAPIS_SERVICE_INITIALIZATION_FAILED", 
                                          "CreateDatabase", e.getMessage());
              _log.error(msg, e);
              throw e;
          }
             
        // Start processing.
        CreateDatabase cdb = new CreateDatabase(parms);
        cdb.create();
        
        // Success.
        System.out.println("\n--> Created database " + parms.tapisDB + " on host " +
                           parms.dbHost + " with owner " + parms.dbUser + ".");
    }
    
    /* ---------------------------------------------------------------------------- */
    /* create:                                                                      */
    /* ---------------------------------------------------------------------------- */
    public void create() throws SQLException, TapisException
    {
        // Connect to the admin database.
        Connection adminConn = connectDB(_parms.adminDB);
        
        // Create the database with the creator as owner.
        createDB(adminConn);
        
        // Close the connection.
        adminConn.close();
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* connectDB:                                                                   */
    /* ---------------------------------------------------------------------------- */
    private Connection connectDB(String dbName) 
     throws SQLException, TapisException
    {
        // Get a tapis datasource.
        DataSource ds = 
            TapisDataSource.getDataSource(
                getClass().getSimpleName(), 
                ADMIN_POOL, 
                TapisDBUtils.makeJdbcUrl(_parms.dbHost, _parms.dbPort, _parms.adminDB),
                _parms.dbUser, 
                _parms.dbPwd, 
                MAX_POOL_SIZE, 
                0);
        
        
        // Get the connection.
        Connection connection = ds.getConnection();
        return connection;
    }

    /* ---------------------------------------------------------------------------- */
    /* createDB:                                                                    */
    /* ---------------------------------------------------------------------------- */
    private void createDB(Connection adminConn) throws SQLException
    {
        // This is how one runs data non-sql statement outside of transactions.
        adminConn.setAutoCommit(true);
        
        Statement stmt = adminConn.createStatement();
        String sql = "CREATE DATABASE " + _parms.tapisDB +
                     " WITH OWNER " + _parms.dbUser +
                     " ENCODING='UTF8' LC_COLLATE='en_US.utf8' LC_CTYPE='en_US.utf8'";
        stmt.execute(sql);
        stmt.close();
        
        // Reset the connection.
        adminConn.setAutoCommit(false);
    }
}
