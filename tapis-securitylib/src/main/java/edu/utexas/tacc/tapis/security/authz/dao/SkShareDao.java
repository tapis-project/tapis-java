package edu.utexas.tacc.tapis.security.authz.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.authz.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.security.authz.model.SkShare;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Lightweight DAO that uses the caller's datasource to connect to the 
 * database.  If this subproject becomes its own service, then it will
 * configure and use its own datasource.  See Jobs for an example on
 * how to do this.
 */
public final class SkShareDao
 extends SkAbstractDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SkShareDao.class);
  
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
  public SkShareDao() throws TapisException {}
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* shareResource:                                                         */
  /* ---------------------------------------------------------------------- */
  public int shareResource(SkShare skshare) throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(skshare.getTenant())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "tenant");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(skshare.getGrantor())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "grantor");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(skshare.getGrantee())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "grantee");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(skshare.getResourceType())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "resourceType");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(skshare.getResourceId1())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "resourceId1");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(skshare.getPrivilege())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "privilege");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(skshare.getCreatedBy())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "createdBy");
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(skshare.getCreatedByTenant())) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "createdByTenant");
          throw new TapisException(msg);
      }
      
      // Assign timestamp.
      skshare.setCreated(Instant.now());
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      int rows = 0;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Set the sql command.
          String sql = SqlStatements.SHARE_INSERT;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, skshare.getTenant());
          pstmt.setString(2, skshare.getGrantor());
          pstmt.setString(3, skshare.getGrantee());
          pstmt.setString(4, skshare.getResourceType());
          pstmt.setString(5, skshare.getResourceId1());
          pstmt.setString(6, skshare.getResourceId2());
          pstmt.setString(7, skshare.getPrivilege());
          pstmt.setTimestamp(8, Timestamp.from(skshare.getCreated()));
          pstmt.setString(9, skshare.getCreatedBy());
          pstmt.setString(10, skshare.getCreatedByTenant());

          // Issue the call. 0 rows will be returned when a duplicate
          // key conflict occurs--this is not considered an error.
          rows = pstmt.executeUpdate();

          // Commit the transaction.
          pstmt.close();
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_shared");
          _log.error(msg, e);
          throw new TapisException(msg, e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
          if (conn != null)
              try {conn.close();}
              catch (Exception e)
              {
                  // If commit worked, we can swallow the exception.
                  // If not, the commit exception will be thrown.
                  String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                  _log.error(msg, e);
              }
      }
      
      // ------------------------- Get ID ------------------------------
      try {
//          var skshares = getShares();
      }
      catch (Exception e) {
          
      }
      
      return rows;
  }

  /* ---------------------------------------------------------------------- */
  /* getShare:                                                              */
  /* ---------------------------------------------------------------------- */
  public SkShare getShare(String tenant, int id)  throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getShare", "tenant");
          throw new TapisException(msg);
      }
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      SkShare skshare = null;
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Set the sql command.
          String sql = SqlStatements.SHARE_SELECT_BY_ID;

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          pstmt.setInt(2, id);

          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          skshare = populateSkShare(rs);

          // Commit the transaction.
          pstmt.close();
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_shared");
          _log.error(msg, e);
          throw new TapisException(msg, e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
          if (conn != null)
              try {conn.close();}
              catch (Exception e)
              {
                  // If commit worked, we can swallow the exception.
                  // If not, the commit exception will be thrown.
                  String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                  _log.error(msg, e);
              }
      }
      
      // Could be null.
      return skshare;
  }

  /* ---------------------------------------------------------------------- */
  /* getShares:                                                             */
  /* ---------------------------------------------------------------------- */
  public List<SkShare> getShares(String tenant, 
                                 String grantor, 
                                 String grantee,
                                 String resourceType, 
                                 String resourceId1, 
                                 String resourceId2,
                                 String privilege, 
                                 String createdBy, 
                                 String createdByTenant,
                                 Integer id,
                                 Boolean includePublicGrantees,
                                 boolean forceId2ToBeNull)  // null or don't care
   throws TapisException
  {
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getShares", "tenant");
          throw new TapisException(msg);
      }
      
      // ------------------------- ID Query ----------------------------
      // If an id is given it takes precedence over everything else 
      // except tenant and short circuits processing.
      if (id != null) {
          // Call the get-by-id method and return.
          var skshare = getShare(tenant, id);
          var list = new ArrayList<SkShare>(1);
          if (skshare != null) list.add(skshare);
          return list;
      }
      
      // Set default public treatment.
      if (includePublicGrantees == null) includePublicGrantees = Boolean.TRUE;
      
      // ------------------------- Calculate Where ---------------------
      // Where clause set up.
      final int maxParms = 10;
      var whereParms = new ArrayList<String>(maxParms);
      var buf = new StringBuilder();
      
      // Construct the where clause based on the user's arguments.
      whereParms.add("tenant");
      buf.append("WHERE tenant = ? ");
      if (grantor != null) {whereParms.add("grantor"); buf.append("AND grantor = ? ");}
      if (grantee != null) {whereParms.add("grantee"); buf.append("AND grantee = ? ");}
      if (resourceType != null) {whereParms.add("resourceType"); buf.append("AND resource_type = ? ");}
      if (resourceId1 != null) {whereParms.add("resourceId1"); buf.append("AND resource_id1 = ? ");}
      if (resourceId2 != null) {whereParms.add("resourceId2"); buf.append("AND resource_id2 = ? ");}
        else if (forceId2ToBeNull) buf.append("AND resource_id2 IS NULL ");
      if (privilege != null) {whereParms.add("privilege"); buf.append("AND privilege = ? ");}
      if (createdBy != null) {whereParms.add("createdBy"); buf.append("AND createdby = ? ");}
      if (createdByTenant != null) {whereParms.add("createdByTenant"); buf.append("AND createdby_tenant = ? ");}
      
      // We only add a clause when we need to exclude public sharing.
      if (!includePublicGrantees) 
          buf.append("AND grantee NOT IN (\"~public\", \"~public_no_authn\") ");
      
      // Generate the WHERE clause
      var whereClause = buf.toString();
      
      // ------------------------- Call SQL ----------------------------
      Connection conn = null;
      List<SkShare> list = new ArrayList<SkShare>();
      try
      {
          // Get a database connection.
          conn = getConnection();
          
          // Set the sql command.
          String sql = SqlStatements.SHARE_SELECT_DYNAMIC;
          sql = sql.replace(":where", whereClause);

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, tenant);
          
          // Fill in the rest of the where clause values.  The offset
          // is the difference between pstmt index and whereParms index.
          final int offset = 2; 
          for (int i = 0; i < whereParms.size(); i++) {
              var cur = whereParms.get(i);
              if (cur.equals("grantor")) {pstmt.setString(i+offset, grantor); continue;}
              if (cur.equals("grantee")) {pstmt.setString(i+offset, grantee); continue;}
              if (cur.equals("resourceType")) {pstmt.setString(i+offset, resourceType); continue;}
              if (cur.equals("resourceId1")) {pstmt.setString(i+offset, resourceId1); continue;}
              if (cur.equals("resourceId2")) {pstmt.setString(i+offset, resourceId2); continue;}
              if (cur.equals("privilege")) {pstmt.setString(i+offset, privilege); continue;}
              if (cur.equals("createdBy")) {pstmt.setString(i+offset, createdBy); continue;}
              if (cur.equals("createdByTenant")) {pstmt.setString(i+offset, createdByTenant);}
          }

          // Issue the call for the 1 row result set.
          ResultSet rs = pstmt.executeQuery();
          SkShare obj = populateSkShare(rs);
          while (obj != null) {
              list.add(obj);
              obj = populateSkShare(rs);
          }

          // Commit the transaction.
          pstmt.close();
          conn.commit();
      }
      catch (Exception e)
      {
          // Rollback transaction.
          try {if (conn != null) conn.rollback();}
          catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
          
          String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "sk_shared");
          _log.error(msg, e);
          throw new TapisException(msg, e);
      }
      finally {
          // Conditionally return the connection back to the connection pool.
          if (conn != null)
              try {conn.close();}
              catch (Exception e)
              {
                  // If commit worked, we can swallow the exception.
                  // If not, the commit exception will be thrown.
                  String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                  _log.error(msg, e);
              }
      }
      
      return list;
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* populateSkShare:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Populate a new SkShare object with a record retrieved from the 
   * database.  The result set's cursor will be advanced to the next
   * position and, if a row exists, its data will be marshalled into a 
   * SkShare object.  The result set is not closed by this method.
   * 
   * NOTE: This method assumes all fields are returned table definition order.
   * 
   * NOTE: This method must be manually maintained whenever the table schema changes.  
   * 
   * @param rs the unprocessed result set from a query.
   * @return a new model object or null if the result set is null or empty
   * @throws TapisJDBCException on SQL access or conversion errors
   */
  private SkShare populateSkShare(ResultSet rs)
   throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;
    
    try {
      // Return null if the results are empty or exhausted.
      // This call advances the cursor.
      if (!rs.next()) return null;
    }
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    
    // Populate the SkRole object using table definition field order,
    // which is the order specified in all calling methods.
    SkShare obj = new SkShare();
    try {
        obj.setId(rs.getInt(1));
        obj.setTenant(rs.getString(2));
        obj.setGrantor(rs.getString(3));
        obj.setGrantee(rs.getString(4));
        obj.setResourceType(rs.getString(5));
        obj.setResourceId1(rs.getString(6));
        obj.setResourceId2(rs.getString(7));
        obj.setPrivilege(rs.getString(8));
        obj.setCreated(rs.getTimestamp(9).toInstant());
        obj.setCreatedBy(rs.getString(10));
        obj.setCreatedByTenant(rs.getString(11));
    } 
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
      
    return obj;
  }
}
