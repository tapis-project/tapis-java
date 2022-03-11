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
import edu.utexas.tacc.tapis.security.authz.model.SkShareInputFilter;
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
  
  // Public pseudo-grantees.
  public static final String PUBLIC_GRANTEE = "~public";
  public static final String PUBLIC_NO_AUTHN_GRANTEE = "~public_no_authn";
  
  // String used in database to indicate null in non-null columns.
  // This is an easy way to avoid problems involving unique indexes 
  // on nullable columns.  The string value must always replace null
  // just before writing to the database and be replaced by null 
  // just after reading from the database.
  private static final String TAPIS_NULL = "[TAPIS-NULL]";
  
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
          
          // Use our null substitute for id2.
          var id2 = skshare.getResourceId2() == null ? TAPIS_NULL : skshare.getResourceId2();

          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, skshare.getTenant());
          pstmt.setString(2, skshare.getGrantor());
          pstmt.setString(3, skshare.getGrantee());
          pstmt.setString(4, skshare.getResourceType());
          pstmt.setString(5, skshare.getResourceId1());
          pstmt.setString(6, id2);
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
      // On a best effort basis, get id, created, createdBy and createdByTenant 
      // for new and pre-existing shares. 
      refreshShare(skshare);
      
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
  /** Select a set of shares that conform to the values in the filter map.  
   * The keys enumerations include most share fields plus two flags.  The 
   * TENANT value must always be set.  If the ID value is set, then all other
   * values are ignored and getShare(tenant, id) is called.  
   * 
   * If the INCLUDE_PUBLIC_GRANTEES is true (the default), then the result list 
   * can contain ~public and ~public_no_authn grantees in addition to any specified
   * grantee.
   * 
   * If the REQUIRE_NULL_ID2 is true (the default), then only shares that have
   * a resourceId2 == null will be included in the result list.  If the 
   * RESOURCE_ID2 value is non-null, then REQUIRE_NULL_ID2 is ignored.
   * 
   * @param filter the key/value pairs used to filter the result list.
   * @return the non-null list of 0 or more shares
   * @throws TapisException on error
   */
  public List<SkShare> getShares(SkShareInputFilter filter) 
   throws TapisException
  {
      // ------------------------- Unpack Input ------------------------
      // Don't blow up.
      if (filter == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getShares", "filter");
          throw new TapisException(msg);
      }
      
      // Unpack filter for convenience. All strings except tenant can be null.
      String tenant = filter.getTenant(); 
      String grantor = filter.getGrantor(); 
      String grantee = filter.getGrantee();
      String resourceType = filter.getResourceType(); 
      String resourceId1 = filter.getResourceId1(); 
      String resourceId2 = filter.getResourceId2();
      String privilege = filter.getPrivilege();
      String createdBy = filter.getCreatedBy(); 
      String createdByTenant = filter.getCreatedByTenant();
      int id = filter.getId();
      boolean includePublicGrantees = filter.isIncludePublicGrantees();
      boolean requireNullId2 = filter.isRequireNullId2();
      
      // ------------------------- Check Input -------------------------
      // Exceptions can be throw from here.
      if (StringUtils.isBlank(tenant)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getShares", "tenant");
          throw new TapisException(msg);
      }
      
      // ------------------------- ID Query ----------------------------
      // If a valid id is given it takes precedence over everything  
      // else to short circuits processing.
      if (id > 0) {
          // Call the get-by-id method and return.
          var skshare = getShare(tenant, id);
          var list = new ArrayList<SkShare>(1);
          if (skshare != null) list.add(skshare);
          return list;
      }
      
      // ------------------------- Calculate Where ---------------------
      // Where clause set up.  The whereParms list contains all the optional
      // parameter names the might appear in the where clause.
      final int maxParms = 8; // maximal optional parms
      var whereParms = new ArrayList<String>(maxParms);
      var buf = new StringBuilder();
      
      // Construct the where clause based on the user's arguments.
      buf.append("WHERE tenant = ? "); // Mandatory first clause.
      if (grantor != null) {whereParms.add("grantor"); buf.append("AND grantor = ? ");}
      appendGranteeClause(buf, grantee, whereParms, includePublicGrantees);
      if (resourceType != null) {whereParms.add("resourceType"); buf.append("AND resource_type = ? ");}
      if (resourceId1 != null) {whereParms.add("resourceId1"); buf.append("AND resource_id1 = ? ");}
      if (resourceId2 != null) {whereParms.add("resourceId2"); buf.append("AND resource_id2 = ? ");}
        else if (requireNullId2) buf.append("AND resource_id2 = '" + TAPIS_NULL + "' ");
      if (privilege != null) {whereParms.add("privilege"); buf.append("AND privilege = ? ");}
      if (createdBy != null) {whereParms.add("createdBy"); buf.append("AND createdby = ? ");}
      if (createdByTenant != null) {whereParms.add("createdByTenant"); buf.append("AND createdby_tenant = ? ");}
      
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
  /* appendGranteeClause:                                                   */
  /* ---------------------------------------------------------------------- */
  /** Special case logic that assigns the grantee clause based on the grantee
   * and includePublicGrantees values.
   * 
   * @param buf the where clause being constructed
   * @param grantee the user specified grantee or null
   * @param whereParms the list of placeholder parameters
   * @param includePublicGrantees the user specified public grantee filter
   */
  private void appendGranteeClause(StringBuilder buf, 
                                   String grantee, 
                                   List<String> whereParms, 
                                   boolean includePublicGrantees)
  {
      // Process based on whether a grantee was specified.
      if (grantee == null) {
          // No restrictions on grantee mean no clause is appended at all.
          // Otherwise, we filter out the public pseudo-grantees.
          if (!includePublicGrantees)
              buf.append("AND grantee NOT IN (\"~public\", \"~public_no_authn\") ");
      } else {
          // A grantee is specified, so we determine the exact filter required.
          if (includePublicGrantees) {
              // Handle the special case when the user specifies a public grantee.
              if (PUBLIC_GRANTEE.equals(grantee) || PUBLIC_NO_AUTHN_GRANTEE.equals(grantee))
                  buf.append("AND grantee IN (\"~public\", \"~public_no_authn\") ");
              else {
                  buf.append("AND grantee IN (?, \"~public\", \"~public_no_authn\") ");
                  whereParms.add("grantee"); // Make sure the ? gets replaced.
              }
          } else {
              // Only search for the grantee and exclude the public grantees 
              // (unless the grantee is specified as one of the public grantees).
              buf.append("AND grantee = ? ");
              whereParms.add("grantee"); // Make sure the ? gets replaced.
          }
      }
  }
  
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
    
    // Replace internal representation of null with real null.
    if (TAPIS_NULL.equals(obj.getResourceId2())) obj.setResourceId2(null);
      
    return obj;
  }
  
  /* ---------------------------------------------------------------------- */
  /* refreshShare:                                                          */
  /* ---------------------------------------------------------------------- */
  /** Update an in-memory share object with the latest information from the
   * database.  This method allows the id to be retrieved for just created 
   * shares and also the created, createdBy and createdByTenant to be retrieved
   * for previously existing shares.
   * 
   * The skshare parameter is used for both input and output. Exactly one record
   * is expected to be returned since all seven values are provided that make up 
   * a unique index in sk_shared.
   * 
   * @param skshare the in-memory share updated with database information
   */
  private void refreshShare(SkShare skshare)
  {
      // Use the input share's values to retrieve its database record.
      // The first 7 values define a unique key; the last value eliminates
      // public grantees from the result that are different than the 
      // specified grantee.
      var filter = new SkShareInputFilter();
      filter.setTenant(skshare.getTenant());
      filter.setGrantor(skshare.getGrantor());
      filter.setGrantee(skshare.getGrantee());
      filter.setResourceType(skshare.getResourceType());
      filter.setResourceId1(skshare.getResourceId1());
      filter.setResourceId2(skshare.getResourceId2());
      filter.setPrivilege(skshare.getPrivilege());
      filter.setIncludePublicGrantees(false);
      
      try {
          // Retrieve from the database. There should be 
          // exactly one share returned.
          var list = getShares(filter);
          if (list != null && list.size() == 1) {
              var dbShare = list.get(0);
              skshare.setId(dbShare.getId());
              skshare.setCreated(dbShare.getCreated());
              skshare.setCreatedBy(dbShare.getCreatedBy());
              skshare.setCreatedByTenant(dbShare.getCreatedByTenant());
          }
          else {
              int size = list == null ? 0 : list.size();
              var id2 = skshare.getResourceId2() == null ? "null" : skshare.getResourceId2();
              _log.warn(MsgUtils.getMsg("JOBS_SHARE_LIST_LEN", 1, size,
                                         skshare.getTenant(),
                                         skshare.getGrantor(),
                                         skshare.getGrantee(),
                                         skshare.getResourceType(),
                                         skshare.getResourceId1(),
                                         id2,
                                         skshare.getPrivilege()));
          }
      } 
      catch (Exception e) {_log.warn(e.getMessage(), e);} // log then swallow exception
  }
}
