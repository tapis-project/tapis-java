package edu.utexas.tacc.tapis.jobs.dao;

import static edu.utexas.tacc.tapis.search.SearchUtils.SearchOperator.CONTAINS;
import static edu.utexas.tacc.tapis.search.SearchUtils.SearchOperator.NCONTAINS;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.OrderField;
import org.jooq.Result;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.sql.SqlStatements;
import edu.utexas.tacc.tapis.jobs.events.JobEventManager;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.gen.jooq.Tables;
import edu.utexas.tacc.tapis.jobs.gen.jooq.tables.records.JobsRecord;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.dto.JobListDTO;
import edu.utexas.tacc.tapis.jobs.model.dto.JobStatusDTO;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobRemoteOutcome;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobType;
import edu.utexas.tacc.tapis.jobs.model.submit.JobSharedAppCtx.JobSharedAppCtxEnum;
import edu.utexas.tacc.tapis.jobs.statemachine.JobFSMUtils;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.search.SearchUtils.SearchOperator;
import edu.utexas.tacc.tapis.search.parser.ASTBinaryExpression;
import edu.utexas.tacc.tapis.search.parser.ASTLeaf;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTUnaryExpression;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisNotFoundException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;


/** A note about querying our JSON data types.  The jobs database schema currently defines these 
 * fields as jsonb:  inputs, parameters, execSystemConstraints and notifications.  See the flyway 
 * scripts in tapis-jobsmigrate for the complete definition.  The SubmitJobRequest.json schema in 
 * tapis-jobsapi defines the json dchema that validates job submission requests.  
 * 
 * The jsonb database type allows for indexed searches of json data.  Initially, only one json 
 * index is defined on the exec_system_constraints field.  All searches of json data that do not 
 * select this index will cause a full table scan if no other index is employed.
 * 
 * Here is the json GIN index defined on the jobs exec_system_constraints field:
 * 
 *  I1: CREATE INDEX jobs_exec_sys_constraints_idx ON jobs USING GIN ((exec_system_constraints));
 * 
 * An example of the type of query to which the index will be applied is:
 * 
 * USE:
 *  Q1: Select * from jobs where exec_system_constraints @@ '$.execSystemConstraints[*].key == "key1"'
 * 
 * This query uses the jsonpath predicate operator, @@, which evaluates an expression that includes
 * a jsonpath.  The above query could have had the ::jsonpath type appended to the end.  
 * 
 * Note that execSystemConstraints is an array of json objects.  The specification of the @@ operator 
 * in the query triggers the use of index I1.  Also note the very particular syntax that must be
 * used to activate the index:  Any of "key", "op" and/or "value" can be included in the path filter 
 * as they are all valid components of constraint objects.
 * 
 * Other json operators such as the containment operator, @>, will not trigger the use of index I1
 * and will result in a full table scan unless there's another where clause that uses an index.
 * 
 * DON'T USE:
 *  Q2: Select * from jobs where where exec_system_constraints -> 'execSystemConstraints' @> '[{"key": "key1"}]'
 * 
 * Alternate Approach (not implemented)
 * ------------------------------------
 * An alternative approach would embed execSystemConstraints in the existing parameters column.  
 * In this case, we would use a json GIN EXPRESSION index defined on the parameters field: 
 * 
 * 	I2:	CREATE INDEX jobs_exec_sys_constraints_idx ON jobs USING GIN ((parameters -> 'execSystemConstraints'));
 * 
 * An example of the type of query to which the index will be applied is:
 * 
 * 	Q3:	Select * from jobs where parameters -> 'execSystemConstraints' @> '[{"key": "key1"}]'::jsonb;
 *
 * The use of the GIN EXPRESSION index rather than a simple index on a whole column has several side 
 * effects.  On the positive side, expression indexes are often smaller and faster.  In the negative side, 
 * indexed searches are limited to the execSystemConstraints document subtree and only certain operators 
 * will trigger indexed searches.  In particular, query Q1 uses index I1 but not I2; Q3 uses I2 but not 
 * whole column indexes like I1. 
 *
 * Expression indexing was not chosen because the queries it requires seem less intuitive than the 
 * jsonpath queries that use full column indexing.
 * 
 * The postgres support for json is extensive but somewhat complicated to get right.
 * See https://www.postgresql.org/docs/12/datatype-json.html
 * 
 * @author rcardone
 */
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
	
	// Message when creating job.
	private static final String JOB_CREATE_MSG = "Job created";
	  
    // Comma-separated string of non active statuses ready for sql query.
    private final static String _nonActiveWithPendingJobStatuses = JobStatusType.getNonActiveWithPendingSQLString();
    
    // Comma-separated string of non active statuses ready for sql query.
    private final static String _nonActiveWithoutPendingJobStatuses = JobStatusType.getNonActiveWithoutPendingSQLString();
    
    // Comma-separated string of terminal statuses ready for sql query.
    private final static String _terminalStatuses = JobStatusType.getTerminalSQLString();
    
    // comma space string to be appended during ORDER BY SQL clause statement preparation
    private static final String SUFFIX_COMMA_SPACE = ", ";
    
    // Table name from which columns names are retrieved
    private static final String JOBS_TABLENAME = "jobs";
    
    // Default orderBy field value
    private static final String DEFAULT_ORDER_BY = "lastUpdated";
    
    // Initialize Jobs Table Map with column name and type;
    public static final Map<String, String> JOB_REQ_DB_MAP = initializeJobFieldMap();
    
    /* ********************************************************************** */
    /*                                 Enums                                  */
    /* ********************************************************************** */
    // Determine which file transfer value is updated.
    public enum TransferValueType {InputTransferId, InputCorrelationId, 
                                   ArchiveTransferId, ArchiveCorrelationId}
    
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
	/* getJobs:                                                               */
	/* ---------------------------------------------------------------------- */
	public List<Job> getJobs() 
	  throws JobException
	{
	    // Initialize result.
	    ArrayList<Job> list = new ArrayList<>();
     
	    // ------------------------- Call SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	          Job obj = populateJob(rs);
	          while (obj != null) {
	            list.add(obj);
	            obj = populateJob(rs);
	          }
	          
	          // Close the result and statement.
	          rs.close();
	          pstmt.close();
	    
	          // Commit the transaction.
	          conn.commit();
	      }
	      catch (Exception e)
	      {
	          // Rollback transaction.
	          try {if (conn != null) conn.rollback();}
	              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
	          throw new JobException(msg, e);
	      }
	      finally {
	          // Always return the connection back to the connection pool.
	          try {if (conn != null) conn.close();}
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
	
	/* ---------------------------------------------------------------------- */
	/* getJobsByUsername:                                                     */
	/* ---------------------------------------------------------------------- */
	public List<JobListDTO> getJobsByUsername(String username, String tenant, 
	                                          List<OrderBy> orderByList, 
	                                          Integer limit, Integer skip)
	 throws JobException
	{
	    // Initialize result.
	    ArrayList<JobListDTO> jobList = new ArrayList<>();
     
	    // ------------------------- Call SQL ----------------------------
	    Connection conn = null;
	    try
	    {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS_BY_USERNAME;
	          String orderBy="";
	          int listsize = orderByList.size();
	         
	          for (int i = 0;i < listsize; i++) {
	              
	        	  if (orderBy.isBlank()) {
	        		  orderBy = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
	        	  } else {
	        		 orderBy = orderBy + " " + SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
	        	  }
	        	   orderBy =  orderBy + " " + orderByList.get(i).getOrderByDir().toString() + SUFFIX_COMMA_SPACE;
	          }
	          orderBy = StringUtils.stripEnd(orderBy, SUFFIX_COMMA_SPACE);
	          
	          if (orderBy.isBlank()) {
	        	  orderBy = SearchUtils.camelCaseToSnakeCase(DEFAULT_ORDER_BY);
	          }
	          
	          sql = sql.replace(":orderby", orderBy);
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	          pstmt.setString(1, username);
	          pstmt.setString(2, tenant);
	          pstmt.setBoolean(3, true); //visible is set to true
	          
	          pstmt.setInt(4, limit);
	          pstmt.setInt(5, skip);
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	         
	          // Quick check.
	  	      if (rs == null) return null;
	  	    
	  	      try {
	  	    	  // Return null if the results are empty or exhausted.
	  	    	  // This call advances the cursor.
	  	    	  if (!rs.next()) return null;
	  	      }
	  	      catch (Exception e) {
	  	    	  String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
	  	    	  throw new TapisJDBCException(msg, e);
	  	      }
	  	      
	          // JobList for specific user.
	  	      JobListDTO jobListObject ;
	  	      do {
                jobListObject = new JobListDTO();
                jobListObject.setUuid(rs.getString(1));
                jobListObject.setTenant(tenant);
                jobListObject.setName(rs.getString(3));
                jobListObject.setOwner(rs.getString(4));
                jobListObject.setStatus(JobStatusType.valueOf(rs.getString(5)));
                Timestamp ts = rs.getTimestamp(6);
                if (ts != null) 
                    jobListObject.setCreated(ts.toInstant());
                
                ts = rs.getTimestamp(7);
                if (ts != null) jobListObject.setEnded(ts.toInstant());
                
                ts = rs.getTimestamp(8);
                if (ts != null) jobListObject.setLastUpdated(ts.toInstant());
                
                jobListObject.setAppId(rs.getString(9));
                jobListObject.setAppVersion(rs.getString(10));
                jobListObject.setExecSystemId(rs.getString(11));
                jobListObject.setArchiveSystemId(rs.getString(11));
                ts = rs.getTimestamp(13);
                
                if (ts != null) jobListObject.setRemoteStarted(ts.toInstant());
                
                jobList.add(jobListObject);
                
             } while(rs.next()) ;
                     
              // Close the result and statement.
	          rs.close();
	          pstmt.close();
	    
	          // Commit the transaction.
	          conn.commit();
	      }
	      catch (Exception e)
	      {
	          // Rollback transaction.
	          try {if (conn != null) conn.rollback();}
	              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
	          throw new JobException(msg, e);
	      }
	      finally {
	          // Always return the connection back to the connection pool.
	          try {if (conn != null) conn.close();}
	            catch (Exception e) 
	            {
	              // If commit worked, we can swallow the exception.  
	              // If not, the commit exception will be thrown.
	              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
	              _log.error(msg, e);
	            }
	      }
	      
	      return jobList;
	}
	
	/* ---------------------------------------------------------------------- */
	/* getJobsSearchListCountByUsername:                                      */
	/* ---------------------------------------------------------------------- */
	@SuppressWarnings("rawtypes")
	public int getJobsSearchListCountByUsername(String username, String tenant,
	                                            List<String> searchList, List<OrderBy> orderByList,
	                                            boolean sharedWithMe) 
      throws TapisException
	{
		int listsize = orderByList.size();
	    _log.debug("listsize: " + listsize);
	    
        for(int i = 0;i < listsize; i++) {
        	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
        	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
        	if(orderByList.get(i)!=null && colOrderBy == null) {
        		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
        		throw new TapisException(msg);
        	}
        }
        Condition whereCondition = null;
        if(sharedWithMe) {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.VISIBLE.eq(true)); // username is not the owner
        } else {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        }
      	if(searchList != null) {
      		whereCondition = addSearchListToWhere(whereCondition, searchList);
      	}
      	List<OrderField> orderList = new ArrayList<OrderField>();
      	if(orderByList != null) {
      		for(int i = 0;i < listsize; i++) {
            	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
            	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
            	if(orderByList.get(i)!=null && colOrderBy == null) {
            		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
            		throw new TapisException(msg);
            	}
            	if(orderByList.get(i).getOrderByDir().name().equals("ASC")) {
            		orderList.add(colOrderBy.asc());
            	}else {
            		orderList.add(colOrderBy.desc());
            	}
          	}
          }
      		
        // ------------------------- Build and execute SQL ----------------------------
      	int count = 0;
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          DSLContext db = DSL.using(conn);

	          // Execute the select including orderByAttrList, startAfter
	          count = db.selectCount().from(Tables.JOBS).where(whereCondition).fetchOne(0,int.class);

	          // Close out and commit
	          if ((conn !=null)) conn.commit();
	        }
	        catch (Exception e)
	        {
	        	 // Rollback transaction.
		          try {if (conn != null) conn.rollback();}
		              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
		          
		          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
		          throw new JobException(msg, e);
	        }
	        finally
	        {
	          // Always return the connection back to the connection pool.
	        	 // Always return the connection back to the connection pool.
		          try {if (conn != null) conn.close();}
		            catch (Exception e) 
		            {
		              // If commit worked, we can swallow the exception.  
		              // If not, the commit exception will be thrown.
		              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
		              _log.error(msg, e);
		            }
	        }
	    return count;
    }
	
	
	/* ---------------------------------------------------------------------- */
	/* getJobsSearchListCountByUsernameUsingSqlSearchStr                      */
	/* ---------------------------------------------------------------------- */
	@SuppressWarnings("rawtypes")
	public int getJobsSearchListCountByUsernameUsingSqlSearchStr(String username, String tenant, ASTNode searchAST, 
			List<OrderBy> orderByList, boolean sharedWithMe) 
			  throws TapisException
	{
		int listsize = orderByList.size();
	    _log.debug("listsize: " + listsize);
	   
	    
        for(int i = 0;i < listsize; i++) {
        	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
        	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
        	if(orderByList.get(i)!=null && colOrderBy == null) {
        		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
        		throw new TapisException(msg);
        	}
        	
        }
      	 
        
        //Condition whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        
        Condition whereCondition = null;
        if(sharedWithMe) {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.VISIBLE.eq(true)); // username is not the owner
        } else {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        }
      	     
        if(searchAST != null) {
      		Condition astCondition = createConditionFromAst(searchAST);
            if (astCondition != null) whereCondition = whereCondition.and(astCondition);
            
      	}
       
      	List<OrderField> orderList = new ArrayList<OrderField>();
      	if(orderByList != null) {
      		for(int i = 0;i < listsize; i++) {
            	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
            	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
            	if(orderByList.get(i)!=null && colOrderBy == null) {
            		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
            		throw new TapisException(msg);
            	}
            	if(orderByList.get(i).getOrderByDir().name().equals("ASC")) {
            		orderList.add(colOrderBy.asc());
            	}else {
            		orderList.add(colOrderBy.desc());
            	}
          	}
        }
      	// ------------------------- Build and execute SQL ----------------------------
      	int count = 0;
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          DSLContext db = DSL.using(conn);

	          // Execute the select including orderByAttrList, startAfter
	          count = db.selectCount().from(Tables.JOBS).where(whereCondition).fetchOne(0,int.class);

	          // Close out and commit
	          if ((conn !=null)) conn.commit();
	        }
	        catch (Exception e)
	        {
	        	 // Rollback transaction.
		          try {if (conn != null) conn.rollback();}
		              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
		          
		          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
		          throw new JobException(msg, e);
	        }
	        finally
	        {
	          // Always return the connection back to the connection pool.
	        	 // Always return the connection back to the connection pool.
		          try {if (conn != null) conn.close();}
		            catch (Exception e) 
		            {
		              // If commit worked, we can swallow the exception.  
		              // If not, the commit exception will be thrown.
		              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
		              _log.error(msg, e);
		            }
	        }
	    return count;
    }
	
	/* ---------------------------------------------------------------------- */
	/* getJobsSearchByUsername:                                               */
	/*  summary attributes                                                    */
	/* ---------------------------------------------------------------------- */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<JobListDTO> getJobsSearchByUsername(String username, String tenant, 
	                                  List<String>searchList, List<OrderBy> orderByList, 
	                                  Integer limit, Integer skip, boolean sharedWithMe) 
	  throws TapisException
	{
	    // Initialize result.
	    ArrayList<JobListDTO> jobList = new ArrayList<>();
       
	    // Negative skip indicates no skip
	    if (skip < 0) skip = 0;
	    
	   
	    
	    int listsize = orderByList.size();
	    _log.debug("listsize: " + listsize);
	    
	    
        for(int i = 0;i < listsize; i++) {
        	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
        	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
        	if(orderByList.get(i)!=null && colOrderBy == null) {
        		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
        		throw new TapisException(msg);
        	}
        	
        }
        Condition whereCondition = null;
        if(sharedWithMe) {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.VISIBLE.eq(true));
        } else {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        }
      	if(searchList != null) {
      		whereCondition = addSearchListToWhere(whereCondition, searchList);
      	}
      	List<OrderField> orderList = new ArrayList<OrderField>();
      	if(orderByList != null) {
      		for(int i = 0;i < listsize; i++) {
            	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
            	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
            	if(orderByList.get(i)!=null && colOrderBy == null) {
            		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
            		throw new TapisException(msg);
            	}
            	if(orderByList.get(i).getOrderByDir().name().equals("ASC")) {
            		orderList.add(colOrderBy.asc());
            	}else {
            		orderList.add(colOrderBy.desc());
            	}
          	}
            	
         }
	 
      	// Build list of attributes we will be returning.
        List<TableField> fieldList = new ArrayList<>();
        fieldList.add(Tables.JOBS.UUID);
        fieldList.add(Tables.JOBS.TENANT);
        fieldList.add(Tables.JOBS.NAME);
        fieldList.add(Tables.JOBS.OWNER);
        fieldList.add(Tables.JOBS.STATUS);
        fieldList.add(Tables.JOBS.CREATED);
        fieldList.add(Tables.JOBS.ENDED);
        fieldList.add(Tables.JOBS.LAST_UPDATED);
        fieldList.add(Tables.JOBS.APP_ID);
        fieldList.add(Tables.JOBS.APP_VERSION);
        fieldList.add(Tables.JOBS.EXEC_SYSTEM_ID);
        fieldList.add(Tables.JOBS.ARCHIVE_SYSTEM_ID);
        fieldList.add(Tables.JOBS.REMOTE_STARTED);
	    
	    // ------------------------- Build and execute SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          DSLContext db = DSL.using(conn);

	          // Execute the select including limit, orderByAttrList, skip and startAfter
	          // NOTE: LIMIT + OFFSET is not standard among DBs and often very difficult to get right.
	          //       Jooq claims to handle it well.
	          Result<JobsRecord> results;
	          org.jooq.SelectConditionStep condStep = db.select(fieldList).from(Tables.JOBS).where(whereCondition);
	          if(orderByList != null && limit >= 0) {
	        	  results = condStep.orderBy(orderList).limit(limit).offset(skip).fetchInto(Tables.JOBS);  
	          } else if (limit >= 0) {
	            // We are limiting but not ordering
	            results = condStep.limit(limit).offset(skip).fetchInto(Tables.JOBS);
	          } else if(orderByList != null && limit == -1) {
	        	  results = condStep.orderBy(orderList).offset(skip).fetchInto(Tables.JOBS); 
	          }
	          else
	          {
	            // We are not limiting and not ordering
	            results = condStep.fetchInto(Tables.JOBS);
	          }

	          if (results == null || results.isEmpty()) return jobList;

	          // Create SystemBasic objects from TSystem objects.
	          for (JobsRecord r : results)
	          {
	            JobListDTO job = r.into(JobListDTO.class);
	            jobList.add(job);
	          }
	          
	          

	          // Close out and commit
	          if ((conn !=null)) conn.commit();
	        }
	        catch (Exception e)
	        {
	        	 // Rollback transaction.
		          try {if (conn != null) conn.rollback();}
		              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
		          
		          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
		          throw new JobException(msg, e);
	        }
	        finally
	        {
	          // Always return the connection back to the connection pool.
	        	 // Always return the connection back to the connection pool.
		          try {if (conn != null) conn.close();}
		            catch (Exception e) 
		            {
		              // If commit worked, we can swallow the exception.  
		              // If not, the commit exception will be thrown.
		              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
		              _log.error(msg, e);
		            }
	        }
	        return jobList;
	}
	
	/* ---------------------------------------------------------------------- */
	/* getJobSearchListByUsernameUsingSqlSearchStr:                           */
	/* summary attributes  post end-point                                     */
	/* ---------------------------------------------------------------------- */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<JobListDTO> getJobSearchListByUsernameUsingSqlSearchStr(
	                         String username, String tenant, ASTNode searchAST, 
			                 List<OrderBy> orderByList,Integer limit, Integer skip, boolean shared) 
     throws TapisException
	{
		 // Initialize result.
	    ArrayList<JobListDTO> jobList = new ArrayList<>();
       
	    // Negative skip indicates no skip
	    if (skip < 0) skip = 0;
	    
	   
	    
	    int listsize = orderByList.size();
	    _log.debug("listsize: " + listsize);
	    
	    
        for(int i = 0;i < listsize; i++) {
        	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
        	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
        	if(orderByList.get(i)!=null && colOrderBy == null) {
        		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
        		throw new TapisException(msg);
        	}
        }
      	 
        //Condition whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        Condition whereCondition  = null;
        if(shared) {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.VISIBLE.eq(true));
        } else {
        	whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        }
      	if(searchAST != null) {
      		Condition astCondition = createConditionFromAst(searchAST);
            if (astCondition != null) whereCondition = whereCondition.and(astCondition);
            
      	}
      	List<OrderField> orderList = new ArrayList<OrderField>();
      	if(orderByList != null) {
      		for(int i = 0;i < listsize; i++) {
            	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
            	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
            	if(orderByList.get(i)!=null && colOrderBy == null) {
            		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
            		throw new TapisException(msg);
            	}
            	if(orderByList.get(i).getOrderByDir().name().equals("ASC")) {
            		orderList.add(colOrderBy.asc());
            	}else {
            		orderList.add(colOrderBy.desc());
            	}
          	}
            	
            }
	 
        // Build list of attributes we will be returning.
        List<TableField> fieldList = new ArrayList<>();
        fieldList.add(Tables.JOBS.UUID);
        fieldList.add(Tables.JOBS.TENANT);
        fieldList.add(Tables.JOBS.NAME);
        fieldList.add(Tables.JOBS.OWNER);
        fieldList.add(Tables.JOBS.STATUS);
        fieldList.add(Tables.JOBS.CREATED);
        fieldList.add(Tables.JOBS.ENDED);
        fieldList.add(Tables.JOBS.LAST_UPDATED);
        fieldList.add(Tables.JOBS.APP_ID);
        fieldList.add(Tables.JOBS.APP_VERSION);
        fieldList.add(Tables.JOBS.EXEC_SYSTEM_ID);
        fieldList.add(Tables.JOBS.ARCHIVE_SYSTEM_ID);
        fieldList.add(Tables.JOBS.REMOTE_STARTED);
	    
	    // ------------------------- Build and execute SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          DSLContext db = DSL.using(conn);

	          // Execute the select including limit, orderByAttrList, skip and startAfter
	          // NOTE: LIMIT + OFFSET is not standard among DBs and often very difficult to get right.
	          //       Jooq claims to handle it well.
	          Result<JobsRecord> results;
	          org.jooq.SelectConditionStep condStep = db.select(fieldList).from(Tables.JOBS).where(whereCondition);
	          if(orderByList != null && limit >= 0) {
	        	  results = condStep.orderBy(orderList).limit(limit).offset(skip).fetchInto(Tables.JOBS);  
	          } else if (limit >= 0) {
	            // We are limiting but not ordering
	            results = condStep.limit(limit).offset(skip).fetchInto(Tables.JOBS);
	          }
	          else
	          {
	            // We are not limiting and not ordering
	            results = condStep.fetchInto(Tables.JOBS);
	          }

	          if (results == null || results.isEmpty()) return jobList;

	          // Create SystemBasic objects from TSystem objects.
	          for (JobsRecord r : results)
	          {
	            JobListDTO job = r.into(JobListDTO.class);
	            jobList.add(job);
	          }
	          
	          

	          // Close out and commit
	          if ((conn !=null)) conn.commit();
	        }
	        catch (Exception e)
	        {
	        	 // Rollback transaction.
		          try {if (conn != null) conn.rollback();}
		              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
		          
		          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
		          throw new JobException(msg, e);
	        }
	        finally
	        {
	          // Always return the connection back to the connection pool.
	        	 // Always return the connection back to the connection pool.
		          try {if (conn != null) conn.close();}
		            catch (Exception e) 
		            {
		              // If commit worked, we can swallow the exception.  
		              // If not, the commit exception will be thrown.
		              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
		              _log.error(msg, e);
		            }
	        }
	        return jobList;
		
	}
	
	/* ---------------------------------------------------------------------- */
	/* getJobSearchAllAttributesByUsername:                                   */
	/*  all attributes                                                        */
	/* ---------------------------------------------------------------------- */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<Job> getJobSearchAllAttributesByUsername(String username, String tenant, List<String>searchList, 
			List<OrderBy> orderByList,Integer limit, Integer skip, boolean shared) 
	  throws TapisException
	{
	    // Initialize result.
	    ArrayList<Job> jobs = new ArrayList<>();
       
	    // Negative skip indicates no skip
	    if (skip < 0) skip = 0;
	    
	   
	    
	    int listsize = orderByList.size();
	    _log.debug("listsize: " + listsize);
	    
	    
        for(int i = 0;i < listsize; i++) {
        	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
        	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
        	if(orderByList.get(i)!=null && colOrderBy == null) {
        		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
        		throw new TapisException(msg);
        	}
        	
        }
        Condition whereCondition;
      	if(shared) {
      		whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.VISIBLE.eq(true));
      	} else {
      		whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        }
      	if(searchList != null) {
      		whereCondition = addSearchListToWhere(whereCondition, searchList);
      	}
      	List<OrderField> orderList = new ArrayList<OrderField>();
      	if(orderByList != null) {
      		for(int i = 0;i < listsize; i++) {
            	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
            	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
            	if(orderByList.get(i)!=null && colOrderBy == null) {
            		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
            		throw new TapisException(msg);
            	}
            	if(orderByList.get(i).getOrderByDir().name().equals("ASC")) {
            		orderList.add(colOrderBy.asc());
            	}else {
            		orderList.add(colOrderBy.desc());
            	}
          	}
            	
       }
   
	    // ------------------------- Build and execute SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          DSLContext db = DSL.using(conn);

	          // Execute the select including limit, orderByAttrList, skip and startAfter
	          // NOTE: LIMIT + OFFSET is not standard among DBs and often very difficult to get right.
	          //       Jooq claims to handle it well.
	          Result<JobsRecord> results;
	          org.jooq.SelectConditionStep condStep = db.select(DSL.asterisk()).from(Tables.JOBS).where(whereCondition);
	          if(orderByList != null && limit >= 0) {
	        	  results = condStep.orderBy(orderList).limit(limit).offset(skip).fetchInto(Tables.JOBS);  
	          } else if (limit >= 0) {
	            // We are limiting but not ordering
	            results = condStep.limit(limit).offset(skip).fetchInto(Tables.JOBS);
	          }
	          else
	          {
	            // We are not limiting and not ordering
	            results = condStep.fetchInto(Tables.JOBS);
	          }
	          if (results == null || results.isEmpty()) return jobs;

	          // Create Job object from Job objects.
	          for (JobsRecord r : results)
	          {
	        	Job job = r.into(Job.class);
	            jobs.add(job);
	          }

	          // Close out and commit
	          if ((conn !=null)) conn.commit();
	        }
	        catch (Exception e)
	        {
	        	 // Rollback transaction.
		          try {if (conn != null) conn.rollback();}
		              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
		          
		          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
		          throw new JobException(msg, e);
	        }
	        finally
	        {
	          // Always return the connection back to the connection pool.
	        	 // Always return the connection back to the connection pool.
		          try {if (conn != null) conn.close();}
		            catch (Exception e) 
		            {
		              // If commit worked, we can swallow the exception.  
		              // If not, the commit exception will be thrown.
		              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
		              _log.error(msg, e);
		            }
	        }
	        return jobs;
	}
	
	/* ---------------------------------------------------------------------- */
	/* getJobSearchAllAttributesByUsernameUsingSqlSearchStr:                  */
	/*  all attributes                                                        */
	/* ---------------------------------------------------------------------- */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<Job> getJobSearchAllAttributesByUsernameUsingSqlSearchStr(String username, String tenant, ASTNode searchAST, 
			List<OrderBy> orderByList,Integer limit, Integer skip, boolean shared) 
	  throws TapisException
	{
	    // Initialize result.
	    ArrayList<Job> jobs = new ArrayList<>();
       
	    // Negative skip indicates no skip
	    if (skip < 0) skip = 0;
	    
	   
	    
	    int listsize = orderByList.size();
	    _log.debug("listsize: " + listsize);
	    
	    
        for(int i = 0;i < listsize; i++) {
        	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
        	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
        	if(orderByList.get(i)!=null && colOrderBy == null) {
        		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
        		throw new TapisException(msg);
        	}
        	
        }
        Condition whereCondition = null;
      	 
        if(shared) {
      		whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.VISIBLE.eq(true));
      	} else {
      		whereCondition = (Tables.JOBS.TENANT.eq(tenant)).and(Tables.JOBS.OWNER.eq(username)).and(Tables.JOBS.VISIBLE.eq(true));
        }
      	
       	if(searchAST != null) {
      		Condition astCondition = createConditionFromAst(searchAST);
            if (astCondition != null) whereCondition = whereCondition.and(astCondition);
            
      	}
      	List<OrderField> orderList = new ArrayList<OrderField>();
      	if(orderByList != null) {
      		for(int i = 0;i < listsize; i++) {
            	String attr = SearchUtils.camelCaseToSnakeCase(orderByList.get(i).getOrderByAttr());
            	Field<?> colOrderBy = Tables.JOBS.field(DSL.name(attr));
            	if(orderByList.get(i)!=null && colOrderBy == null) {
            		String msg = MsgUtils.getMsg("SEARCH_ORDERBY_DB_NO_COLUMN", DSL.name(attr));
            		throw new TapisException(msg);
            	}
            	if(orderByList.get(i).getOrderByDir().name().equals("ASC")) {
            		orderList.add(colOrderBy.asc());
            	}else {
            		orderList.add(colOrderBy.desc());
            	}
          	}
            	
          }
   
	    // ------------------------- Build and execute SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          DSLContext db = DSL.using(conn);

	          // Execute the select including limit, orderByAttrList, skip and startAfter
	          // NOTE: LIMIT + OFFSET is not standard among DBs and often very difficult to get right.
	          //       Jooq claims to handle it well.
	          Result<JobsRecord> results;
	          org.jooq.SelectConditionStep condStep = db.select(DSL.asterisk()).from(Tables.JOBS).where(whereCondition);
	          if(orderByList != null && limit >= 0) {
	        	  results = condStep.orderBy(orderList).limit(limit).offset(skip).fetchInto(Tables.JOBS);  
	          } else if (limit >= 0) {
	            // We are limiting but not ordering
	            results = condStep.limit(limit).offset(skip).fetchInto(Tables.JOBS);
	          }
	          else
	          {
	            // We are not limiting and not ordering
	            results = condStep.fetchInto(Tables.JOBS);
	          }
	          if (results == null || results.isEmpty()) return jobs;

	          // Create Job object from Job objects.
	          for (JobsRecord r : results)
	          {
	        	Job job = r.into(Job.class);
	            jobs.add(job);
	          }
	          // Close out and commit
	          if ((conn !=null)) conn.commit();
	        }
	        catch (Exception e)
	        {
	        	 // Rollback transaction.
		          try {if (conn != null) conn.rollback();}
		              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
		          
		          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", "allUUIDs", e.getMessage());
		          throw new JobException(msg, e);
	        }
	        finally
	        {
	          // Always return the connection back to the connection pool.
	        	 // Always return the connection back to the connection pool.
		          try {if (conn != null) conn.close();}
		            catch (Exception e) 
		            {
		              // If commit worked, we can swallow the exception.  
		              // If not, the commit exception will be thrown.
		              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
		              _log.error(msg, e);
		            }
	        }
	        return jobs;
	}	

	/* ---------------------------------------------------------------------- */  
    /* getJobByUUID:                                                          */
    /* ---------------------------------------------------------------------- */
	/** Get the specified job or return null if not found.
	 * 
	 * @param uuid the job to retrieve
	 * @return the job object or null if not found
	 * @throws JobException on error (other than job not found)
	 */
    public Job getJobByUUID(String uuid) 
      throws JobException
    {
        // The TapisNotFoundException cannot happen since set the throwNotFound
        // flag as false, but we have account for the exception here to keep it
        // off of the method signature.  All other exceptions pass through to 
        // the caller.
        try {return getJobByUUID(uuid, false);}
            catch (TapisNotFoundException e) {return null;}
    }
    
	/* ---------------------------------------------------------------------- */  
	/* getJobByUUID:                                                          */
	/* ---------------------------------------------------------------------- */
    /** Get the specified job or throw an exception depending on the value of
     * the throwNotFound flag.  If the flag is true and the job is not found,
     * then the TapisNotFoundException exception is thrown.  If the flag is false
     * and the job is not found, null is returned.
     * 
     * @param uuid the job to retrieve
     * @param throwNotFound on not found condition, true means throw exception, 
     *                      false means return null
     * @return the job or null
     * @throws JobException on all errors other than not found
     * @throws TapisNotFoundException on job not found and throwNotFound=true
     */
	public Job getJobByUUID(String uuid, boolean throwNotFound) 
	  throws JobException, TapisNotFoundException
	{
	    // ------------------------- Check Input -------------------------
	    if (StringUtils.isBlank(uuid)) {
	        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobsByUUID", "uuid");
	        throw new JobException(msg);
	    }
	      
	    // Initialize result.
	    Job result = null;

	    // ------------------------- Call SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS_BY_UUID;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	          pstmt.setString(1, uuid);
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	          result = populateJob(rs);
	          
	          // Close the result and statement.
	          rs.close();
	          pstmt.close();
	    
	          // Commit the transaction.
	          conn.commit();
	      }
	      catch (Exception e)
	      {
	          // Rollback transaction.
	          try {if (conn != null) conn.rollback();}
	              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", uuid, e.getMessage());
	          throw new JobException(msg, e);
	      }
	      finally {
	          // Always return the connection back to the connection pool.
	          try {if (conn != null) conn.close();}
	            catch (Exception e) 
	            {
	              // If commit worked, we can swallow the exception.  
	              // If not, the commit exception will be thrown.
	              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
	              _log.error(msg, e);
	            }
	      }
	    
	    // Make sure we found a job.
	    if (result == null && throwNotFound) {
	        String msg = MsgUtils.getMsg("JOBS_JOB_NOT_FOUND", uuid);
	        throw new TapisNotFoundException(msg, uuid);
	    }
	      
	    return result;
	}

	/* ---------------------------------------------------------------------- */  
	/* getJobStatusByUUID:                                                    */
	/* ---------------------------------------------------------------------- */
	public JobStatusDTO getJobStatusByUUID(String uuid) 
	  throws JobException
	{
	    // ------------------------- Check Input -------------------------
	    if (StringUtils.isBlank(uuid)) {
	        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJobsStatusByUUID", "uuid");
	        throw new JobException(msg);
	    }
	     
	    // Initialize result.
	    JobStatusDTO jobStatus = null;

	    // ------------------------- Call SQL ----------------------------
	    Connection conn = null;
	    try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS_STATUS_INFO_BY_UUID;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	          pstmt.setString(1, uuid);
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	         
	          // Quick check.
	  	      if (rs == null) return null;
	  	    
	  	      try {
	  	    	  // Return null if the results are empty or exhausted.
	  	    	  // This call advances the cursor.
	  	    	  if (!rs.next()) return null;
	  	      }
	  	      catch (Exception e) {
	  	    	  String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
	  	    	  throw new TapisJDBCException(msg, e);
	  	      }
	          
	  	      // Extract the status from the result set.
	  	      jobStatus = new JobStatusDTO();
	          jobStatus.setJobUuid(rs.getString(1));
	          jobStatus.setJobId(rs.getInt(2));
	          jobStatus.setOwner(rs.getString(3));
	          jobStatus.setTenant(rs.getString(4));
	          jobStatus.setStatus(JobStatusType.valueOf(rs.getString(5)));
	          jobStatus.setCreatedBy(rs.getString(6));
	          jobStatus.setVisible(rs.getBoolean(7));
	          jobStatus.setCreatedByTenant(rs.getString(8));
	          
	          // Close the result and statement.
	          rs.close();
	          pstmt.close();
	    
	          // Commit the transaction.
	          conn.commit();
	      }
	      catch (Exception e)
	      {
	          // Rollback transaction.
	          try {if (conn != null) conn.rollback();}
	              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", uuid, e.getMessage());
	          throw new JobException(msg, e);
	      }
	      finally {
	          // Always return the connection back to the connection pool.
	          try {if (conn != null) conn.close();}
	            catch (Exception e) 
	            {
	              // If commit worked, we can swallow the exception.  
	              // If not, the commit exception will be thrown.
	              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
	              _log.error(msg, e);
	            }
	      }
	      
	    return jobStatus;
	}

	/* ---------------------------------------------------------------------- */
	/* createJob:                                                             */
	/* ---------------------------------------------------------------------- */
	public void createJob(Job job)
      throws TapisException
	{
        // ------------------------- Complete Input ----------------------
        // Fill in Job fields that we assure.
		if (StringUtils.isBlank(job.getLastMessage())) job.setLastMessage(JOB_CREATE_MSG);
		if (job.getCreated() == null) {
	        Instant now = Instant.now();
	        job.setCreated(now);
	        job.setLastUpdated(now);
		}
        
        // ------------------------- Check Input -------------------------
        // Exceptions can be throw from here.
        validateNewJob(job);
	
        // ------------------------- Call SQL ----------------------------
        JobEvent jobEvent = null;
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Insert into the jobs table first.
          // Create the command using table definition field order.
          String sql = SqlStatements.CREATE_JOB;
          
          // Prepare the statement and fill in the placeholders.
          // The fields that the DB defaults are not set.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, job.getName());
          pstmt.setString(2, job.getOwner());
          pstmt.setString(3, job.getTenant());
          pstmt.setString(4, job.getDescription());
              
          pstmt.setString(5, job.getStatus().name());
              
          pstmt.setString(6, job.getLastMessage());
          pstmt.setTimestamp(7, Timestamp.from(job.getCreated()));
          pstmt.setTimestamp(8, Timestamp.from(job.getLastUpdated()));
              
          pstmt.setString(9, job.getUuid());
            
          pstmt.setString(10, job.getAppId().trim());
          pstmt.setString(11, job.getAppVersion().trim());
          pstmt.setBoolean(12, job.isArchiveOnAppError());
          pstmt.setBoolean(13, job.isDynamicExecSystem());
              
          pstmt.setString(14, job.getExecSystemId());           
          pstmt.setString(15, job.getExecSystemExecDir());      // could be null
          pstmt.setString(16, job.getExecSystemInputDir());     // could be null
          pstmt.setString(17, job.getExecSystemOutputDir());    // could be null
          pstmt.setString(18, job.getExecSystemLogicalQueue()); // could be null
          
          pstmt.setString(19, job.getArchiveSystemId());        // could be null
          pstmt.setString(20, job.getArchiveSystemDir());       // could be null
              
          pstmt.setString(21, job.getDtnSystemId());            // could be null       
          pstmt.setString(22, job.getDtnMountSourcePath());     // could be null
          pstmt.setString(23, job.getDtnMountPoint());          // could be null
          
          pstmt.setInt(24, job.getNodeCount());
          pstmt.setInt(25, job.getCoresPerNode());
          pstmt.setInt(26, job.getMemoryMB());
          pstmt.setInt(27, job.getMaxMinutes());
              
          pstmt.setString(28, job.getFileInputs());                 
          pstmt.setString(29, job.getParameterSet());             
          pstmt.setString(30, job.getExecSystemConstraints());                 
          pstmt.setString(31, job.getSubscriptions());             

          pstmt.setString(32, job.getTapisQueue());
          pstmt.setString(33, job.getCreatedby());
          pstmt.setString(34, job.getCreatedbyTenant());
          
          var tags = job.getTags();
          Array tagsArray;
          if (tags == null || tags.isEmpty()) 
              tagsArray = conn.createArrayOf("text", new String[0]);
            else {
                String[] sarray = tags.toArray(new String[tags.size()]);
                tagsArray = conn.createArrayOf("text", sarray);
            }
          pstmt.setArray(35, tagsArray);
          pstmt.setString(36, job.getJobType().name());
          
          // MPI and command prefix.
          pstmt.setBoolean(37, job.isMpi());
          pstmt.setString(38,  job.getMpiCmd());                // could be null
          pstmt.setString(39,  job.getCmdPrefix());             // could be null
          
          // Shared application context.
          pstmt.setString(40, job.getSharedAppCtx());
          
          // Shared application context attributes.
          var attribs = job.getSharedAppCtxAttribs();
          Array attribsArray;
          if (attribs == null || attribs.isEmpty()) 
              attribsArray = conn.createArrayOf("text", new String[0]);
            else {
                String[] sarray = new String[attribs.size()];
                for (int i = 0; i < attribs.size(); i++) sarray[i] = attribs.get(i).name();
                attribsArray = conn.createArrayOf("text", sarray);
            }
          pstmt.setArray(41, attribsArray);
              
          // Notes is non-null json.
          pstmt.setString(42, job.getNotes());
          
          // Issue the call and clean up statement.
          int rows = pstmt.executeUpdate();
          if (rows != 1) _log.warn(MsgUtils.getMsg("DB_INSERT_UNEXPECTED_ROWS", "jobs", rows, 1));
          pstmt.close();
          
          // Write the event table and issue the notification.
          var eventMgr = JobEventManager.getInstance();
          eventMgr.recordStatusEvent(job, job.getStatus(), null, conn);
    
          // Commit the transaction that may include changes to both tables.
          conn.commit();
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_CREATE_ERROR", job.getName(), 
                                         job.getTenant(), job.getOwner(), e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
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
	}
		   
    /* ---------------------------------------------------------------------- */
    /* getStatusByUUID:                                                       */
    /* ---------------------------------------------------------------------- */
	public JobStatusType getStatusByUUID(String uuid)
	 throws TapisException
	{
	      // ------------------------- Check Input -------------------------
	      if (StringUtils.isBlank(uuid)) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getStatusByUUID", "uuid");
	          throw new TapisException(msg);
	      }
	      
	      // Initialize result.
	      JobStatusType result = null;

	      // ------------------------- Call SQL ----------------------------
	      Connection conn = null;
	      try
	      {
	          // Get a database connection.
	          conn = getConnection();
	          
	          // Get the select command.
	          String sql = SqlStatements.SELECT_JOBS_STATUS_BY_UUID;
	          
	          // Prepare the statement and fill in the placeholders.
	          PreparedStatement pstmt = conn.prepareStatement(sql);
	          pstmt.setString(1, uuid);
	                      
	          // Issue the call for the 1 row result set.
	          ResultSet rs = pstmt.executeQuery();
	          if (rs.next()) {
	              String type = rs.getString(1);
	              result = JobStatusType.valueOf(type);
	          }
	          
	          // Close the result and statement.
	          rs.close();
	          pstmt.close();
	    
	          // Commit the transaction.
	          conn.commit();
	      }
	      catch (Exception e)
	      {
	          // Rollback transaction.
	          try {if (conn != null) conn.rollback();}
	              catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
	          
	          String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "getStatusByUUID", uuid, e.getMessage());
	          _log.error(msg, e);
	          throw new TapisException(msg, e);
	      }
	      finally {
	          // Always return the connection back to the connection pool.
	          try {if (conn != null) conn.close();}
	            catch (Exception e) 
	            {
	              // If commit worked, we can swallow the exception.  
	              // If not, the commit exception will be thrown.
	              String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
	              _log.error(msg, e);
	            }
	      }
	      
	      return result;
	}
	
    /* ---------------------------------------------------------------------- */
    /* getStatusByUUIDSafe:                                                   */
    /* ---------------------------------------------------------------------- */
	/** This method returns the job name and job owner in a JobNameOwner object.
	 * If for any reason the query fails, an empty object is returned with both
	 * values null.  
	 * 
	 * This method never throws an exception.
	 * 
	 * @param uuid
	 * @return
	 */
    public JobNameOwner getNameOwnerByUUIDSafe(String uuid)
    {
          // ---------------------- Initialize Result ----------------------
          // Container for job name and owner.
          var result = new JobNameOwner();  
        
          // ------------------------- Check Input -------------------------
          if (StringUtils.isBlank(uuid)) {
              String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getNameOwnerByUUIDSafe", "uuid");
              _log.error(msg);
              return result;
          }
          
          // ------------------------- Call SQL ----------------------------
          Connection conn = null;
          try
          {
              // Get a database connection.
              conn = getConnection();
              
              // Get the select command.
              String sql = SqlStatements.SELECT_JOBS_NAME_OWNER_BY_UUID;
              
              // Prepare the statement and fill in the placeholders.
              PreparedStatement pstmt = conn.prepareStatement(sql);
              pstmt.setString(1, uuid);
                          
              // Issue the call for the 1 row result set.
              ResultSet rs = pstmt.executeQuery();
              if (rs.next()) {
                  result.name  = rs.getString(1);
                  result.owner = rs.getString(2);
              }
              
              // Close the result and statement.
              rs.close();
              pstmt.close();
        
              // Commit the transaction.
              conn.commit();
          }
          catch (Exception e)
          {
              // Rollback transaction.
              try {if (conn != null) conn.rollback();}
                  catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
              
              String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "getStatusByUUID", uuid, e.getMessage());
              _log.error(msg, e);
          }
          finally {
              // Always return the connection back to the connection pool.
              try {if (conn != null) conn.close();}
                catch (Exception e) 
                {
                  // If commit worked, we can swallow the exception.  
                  // If not, the commit exception will be thrown.
                  String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                  _log.error(msg, e);
                }
          }
          
          return result;
    }
    
	/* ---------------------------------------------------------------------- */
    /* setJobVisibility:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Set the visibility of the job; meaning some requests for job info
     * may exclude this job if visible is set to false.
     *
     * @param jobUuid
     * @param tenant
     * @param user
     * @param isVisible
     * @throws TapisException 
     */
    public void setJobVisibility(String jobUuid, String tenant, String user, boolean isVisible)
        throws TapisException
    {
    	// ------------------------- Check Input -------------------------
	      if (StringUtils.isBlank(jobUuid)) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setJobVisibility", "jobUuid");
	          throw new TapisException(msg);
	      }
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
            // Get a database connection.
            conn = getConnection();

            // Set the sql command.
            String sql = SqlStatements.SET_JOB_VISIBLE;

            // Calculate the new values.
            Instant now = Instant.now();
            Timestamp ts = Timestamp.from(now);

            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setBoolean(1, isVisible);
            pstmt.setTimestamp(2, ts);
            pstmt.setString(3, jobUuid);

            // Issue the call.
            int rows = pstmt.executeUpdate();
            if (rows != 1) {
                String parms = StringUtils.joinWith(", ", "visible", jobUuid);
                String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
                _log.error(msg);
                throw new JobException(msg);
            }

            // Commit the transaction.
            conn.commit();

        } catch(Exception e)
       
         {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            // Close and null out the connection here. This overrides the finally block logic and
            // guarantees that we will not interfere with another thread's use of the connection. 
            try {if (conn != null) conn.close(); conn = null;}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_UPDATE_ERROR", jobUuid, tenant, user, e.getMessage());
                _log.error(msg, e);
                throw new JobException(msg, e);
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
    }
	
    /* ---------------------------------------------------------------------- */
    /* setStatus:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Set the status of the specified job after checking that the transition
     * from the current status to the new status is legal.  This method commits
     * the update and returns the last update time. 
     * 
     * @param uuid the job whose status is to change    
     * @param newStatus the job's new status
     * @param message the status message to be saved in the job record
     * @return the last update time saved in the job record
     * @throws JobException if the status could not be updated
     */
    public Instant setStatus(String uuid, JobStatusType newStatus, String message)
     throws JobException
    {
        // Check input.
        if (StringUtils.isBlank(uuid)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setStatus", "uuid");
            _log.error(msg);
            throw new JobException(msg);
        }

        // Get the job and create its context object for event processing.
        // The new context object is referenced in the job, so it's not garbage.
        Job job = getJobByUUID(uuid);
        Instant ts = setStatus(job, newStatus, message);
        
        return ts;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setStatus:                                                             */
    /* ---------------------------------------------------------------------- */
    /** ALL EXTERNAL STATUS UPDATES AFTER JOB CREATION RUN THROUGH THIS METHOD.
     * 
     * This method sets the status of the specified job after checking that the 
     * transition from the current status to the new status is legal.  This method 
     * commits the update and returns the last update time.
     * 
     * If the message is null, a standard status update message is generated.
     * 
     * Note that a new job event for this status change is persisted and can
     * trigger notifications to be sent.  Failures in event processing are not
     * exposed as errors to callers, they are performed on a best-effort basis.   
     * 
     * @param uuid the job whose status is to change    
     * @param newStatus the job's new status
     * @param message the status message to be saved in the job record or null
     * @return the last update time saved in the job record
     * @throws TapisException 
     */
    public Instant setStatus(Job job, JobStatusType newStatus, String message)
     throws JobException
    {
        // ------------------------- Check Input ------------------------
        // We need a job.
        if (job == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setStatus", "job");
            throw new JobException(msg);
        }
        
        // Assign standard status message if none is provided.
        if (StringUtils.isBlank(message)) 
            message = "Setting job status to " + newStatus.name() + ".";
        
        // ------------------------- Change Status ----------------------
        // Call the real method.
        var oldStatus = job.getStatus();
        Instant now = Instant.now();
        setStatus(job, newStatus, message, true, now);

        // Return the timestamp that's been saved to the database.
        return now;
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateInputTransferTag:                                                */
    /* ---------------------------------------------------------------------- */
    public void updateTransferValue(Job job, String value, TransferValueType type) 
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (job == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateInputTransferTag", "job");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateInputTransferTag", "value");
            throw new JobException(msg);
        }
        
        // Get current time.
        var now = Instant.now();

        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Insert into the jobs table first.
          // Create the command using table definition field order.
          String sql = switch (type) {
              case InputTransferId      -> SqlStatements.UPDATE_INPUT_TRANSFER_ID;
              case InputCorrelationId   -> SqlStatements.UPDATE_INPUT_CORR_ID;
              case ArchiveTransferId    -> SqlStatements.UPDATE_ARCHIVE_TRANSFER_ID;
              case ArchiveCorrelationId -> SqlStatements.UPDATE_ARCHIVE_CORR_ID;
          };
          
          // Prepare the chosen statement.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setTimestamp(1, Timestamp.from(now));
          pstmt.setString(2, value);
          pstmt.setInt(3, job.getId());
          pstmt.setString(4, job.getTenant());
          
          // Issue the call and check that one record was updated.
          int rows = pstmt.executeUpdate();
          if (rows != 1) {
              String parms = StringUtils.joinWith(", ", now, value, job.getId(), job.getTenant());
              String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
              throw new JobException(msg);
          }
             
          // Close the result and statement.
          pstmt.close();
        
          // Commit the transaction.
          conn.commit();
          
          // Update the in-memory job with the latest information.
          job.setLastUpdated(now);
          switch (type) {
              case InputTransferId:      job.setInputTransactionId(value); break;
              case InputCorrelationId:   job.setInputCorrelationId(value); break;
              case ArchiveTransferId:    job.setArchiveTransactionId(value); break;
              case ArchiveCorrelationId: job.setArchiveCorrelationId(value); break;
          }
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_UPDATE_TRANSFER_VALUE_ERROR", job.getUuid(), 
                                         job.getTenant(), job.getOwner(), 
                                         type.name(), value, e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
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
    }
    
    /* ---------------------------------------------------------------------- */
    /* failJob:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the specified job to the terminal FAILED state when the caller does
     * not already have a reference to the job object.  If the caller has a job
     * object, use the overloaded version of this method that accepts a job.
     * 
     * @param caller name of calling program or worker
     * @param jobUuid the job to be failed
     * @param tenantId the job's tenant
     * @param failMsg the message to write to the job record
     * @throws JobException 
     */
    public void failJob(String caller, String jobUuid, String tenantId, String failMsg) 
     throws JobException
    {
        // Make sure we write something to the job record.
        if (StringUtils.isBlank(failMsg)) 
            failMsg = MsgUtils.getMsg("JOBS_STATUS_FAILED_UNKNOWN_CAUSE");
        
        // Fail the job.
        try {setStatus(jobUuid, JobStatusType.FAILED, failMsg);}
            catch (Exception e) {
                // The job will be left in a non-terminal state and probably 
                // removed from any queue.  It's likely to become a zombie.
                String msg = MsgUtils.getMsg("JOBS_WORKER_JOB_UPDATE_ERROR", 
                                              caller, jobUuid, tenantId);
                throw new JobException(msg, e);
            }
    }
    
    /* ---------------------------------------------------------------------- */
    /* failJob:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the specified job to the terminal FAILED state when the caller 
     * already has the job object.  This method is more efficient than the 
     * overloaded version that accepts only a uuid.
     * 
     * @param caller name of calling program or worker
     * @param job the job to be failed
     * @param failMsg the message to write to the job record
     * @throws JobException 
     */
    public void failJob(String caller, Job job, String failMsg) 
     throws JobException
    {
        // Make sure we write something to the job record.
        if (StringUtils.isBlank(failMsg)) 
            failMsg = MsgUtils.getMsg("JOBS_STATUS_FAILED_UNKNOWN_CAUSE");
        
        // Fail the job.
        try {setStatus(job, JobStatusType.FAILED, failMsg);}
            catch (Exception e) {
                // The job will be left in a non-terminal state and probably 
                // removed from any queue.  It's likely to become a zombie.
                String msg = MsgUtils.getMsg("JOBS_WORKER_JOB_UPDATE_ERROR", 
                                              caller, job.getUuid(), job.getTenant());
                throw new JobException(msg, e);
            }
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateLastMessageWithFinalMessage:                                     */
    /* ---------------------------------------------------------------------- */
    /** Update the lastMessage field with the finalMessage                    */
    public void updateLastMessageWithFinalMessage(String finalMessage, Job job)
    {
        
        // Get a database connection.
        Connection conn = null;
        
        // ------------------------- Call SQL ----------------------------
        try {
            
            conn = getConnection(); 
            String sql = SqlStatements.UPDATE_JOB_LAST_MESSAGE;
            
            // Get the variables to be used in the SQL statement. 
            long jobId = job.getId();
            String tenantId = job.getTenant();
            Instant now = Instant.now();
            Timestamp ts = Timestamp.from(now);
            
            // Last message field in the database has a size constraint of 2048 characters.
            // finalMessage needs to fit in this constraint, or it will fail to update the db. 
            if ((finalMessage != null) && (finalMessage.length() > Job.MAX_LAST_MESSAGE_LEN))
                finalMessage = finalMessage.substring(0, Job.MAX_LAST_MESSAGE_LEN - 1);

            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, finalMessage);
            pstmt.setTimestamp(2, ts);
            pstmt.setString(3, tenantId);
            pstmt.setLong(4, jobId);

            // Issue the call.
            int rows = pstmt.executeUpdate();
            if (rows != 1) {
                String msg = MsgUtils.getMsg("DB_LAST_MESSAGE_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, finalMessage);
                throw new JobException(msg);
            }

            // Commit the transaction.
            conn.commit();
        } 
        catch (Exception e) {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            // This a best effort attempt. If it fails, we log and move on without making too much noise. 
            String msg = MsgUtils.getMsg("DB_LAST_MESSAGE_FAILED_UPDATE", e.getMessage());
            _log.error(msg);
        } finally {
            // Always return the connection back to the connection pool.
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
    }
    
    /* ---------------------------------------------------------------------- */
    /* countActiveSystemJobs:                                                 */
    /* ---------------------------------------------------------------------- */
    public int countActiveSystemJobs(String tenantId, String systemId) 
     throws JobException
    {return countActiveSystemJobs(tenantId, systemId, false);}
    
    /** Retrieve the number of jobs in active state on the specified execution
     * system.
     * 
     * @param tenantId the non-null execution system's tenant id
     * @param systemId the non-null execution system's unique id
     * @param pendingActive true means Pending is considered an active state, false means inactive
     * @return the number of aloe jobs active on the specified system
     * @throws JobException 
     */
    public int countActiveSystemJobs(String tenantId, String systemId, boolean pendingActive) 
     throws JobException
    {
        return countActiveJobs(tenantId, systemId, null, null, pendingActive);
    }
    
    /* ---------------------------------------------------------------------- */
    /* countActiveSystemUserJobs:                                             */
    /* ---------------------------------------------------------------------- */
    public int countActiveSystemUserJobs(String tenantId, String systemId, String owner) 
     throws JobException
    {return countActiveSystemUserJobs(tenantId, systemId, owner, false);}
    
    /** Retrieve the number of jobs in active state on the specified execution
     * system.
     * 
     * @param tenantId the non-null execution system's tenant id
     * @param systemId the non-null execution system's unique id
     * @param owner non-null job owner
     * @param pendingActive true means Pending is considered an active state, false means inactive
     * @return the number of aloe jobs active on the specified system
     * @throws JobException 
     */
    public int countActiveSystemUserJobs(String tenantId, String systemId, String owner, boolean pendingActive) 
     throws JobException
    {
        // Only call this method with non-null parms.
        if (StringUtils.isBlank(owner)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "countActiveSystemUserJobs", "owner");
            _log.error(msg);
            throw new JobException(msg);
        }
        
        return countActiveJobs(tenantId, systemId, owner, null, pendingActive);
    }
    
    /* ---------------------------------------------------------------------- */
    /* countActiveSystemQueueJobs:                                            */
    /* ---------------------------------------------------------------------- */
    public int countActiveSystemQueueJobs(String tenantId, String systemId, 
                                          String logicalQueue) 
     throws JobException
    {return countActiveSystemQueueJobs(tenantId, systemId, logicalQueue, false);}
    
    /** Retrieve the number of jobs in active state on the specified execution
     * system.
     * 
     * @param tenantId the non-null execution system's tenant id
     * @param systemId the non-null execution system's unique id
     * @param logicalQueue non-null remote queue
     * @param pendingActive true means Pending is considered an active state, false means inactive
     * @return the number of aloe jobs active on the specified system
     * @throws JobException 
     */
    public int countActiveSystemQueueJobs(String tenantId, String systemId, 
                                          String logicalQueue, boolean pendingActive) 
     throws JobException
    {
        // Only call this method with non-null parms.
        if (StringUtils.isBlank(logicalQueue)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "countActiveSystemQueueJobs", "logicalQueue");
            _log.error(msg);
            throw new JobException(msg);
        }
        
        return countActiveJobs(tenantId, systemId, null, logicalQueue, pendingActive);
    }
    
    /* ---------------------------------------------------------------------- */
    /* countActiveSystemUserQueueJobs:                                        */
    /* ---------------------------------------------------------------------- */
    public int countActiveSystemUserQueueJobs(String tenantId, String systemId, String owner, 
                                              String logicalQueue) 
     throws JobException
    {return countActiveSystemUserQueueJobs(tenantId, systemId, owner, logicalQueue, false);}

    /** Retrieve the number of jobs in active state on the specified execution
     * system.
     * 
     * @param tenantId the non-null execution system's tenant id
     * @param systemId the non-null execution system's unique id
     * @param owner non-null job owner
     * @param logicalQueue non-null remote queue
     * @param pendingActive true means Pending is considered an active state, false means inactive
     * @return the number of aloe jobs active on the specified system
     * @throws JobException 
     */
    public int countActiveSystemUserQueueJobs(String tenantId, String systemId, String owner, 
                                              String logicalQueue, boolean pendingActive) 
     throws JobException
    {
        // Only call this method with non-null parms.
        if (StringUtils.isBlank(owner)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "countActiveSystemUserQueueJobs", "owner");
            _log.error(msg);
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(logicalQueue)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "countActiveSystemUserQueueJobs", "logicalQueue");
            _log.error(msg);
            throw new JobException(msg);
        }
        
        return countActiveJobs(tenantId, systemId, owner, logicalQueue, pendingActive);
    }
    
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

    /* ---------------------------------------------------------------------- */
    /* getTransferInfo:                                                       */
    /* ---------------------------------------------------------------------- */
	/** Get input staging and output archiving information for a job.  The 
	 * transfer and correlations ids are returned.  The result will never be
	 * null, but any or all of the values contained within it can be null.
	 * 
	 * @param jobUuid the job uuid to query
	 * @return the non-null transfer information record
	 * @throws JobException error condition
	 * @throws TapisNotFoundException job not found
	 */
	public JobTransferInfo getTransferInfo(String jobUuid) 
	 throws JobException, TapisNotFoundException
	{
        // ------------------------- Check Input -------------------------
        // Exceptions can be throw from here.
        if (StringUtils.isBlank(jobUuid)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getTransferInfo", "jobUuid");
            throw new JobException(msg);
        }
        
        // Initialize result.
	    JobTransferInfo result = null;

        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
          {
              // Get a database connection.
              conn = getConnection();
              
              // Get the select command.
              String sql = SqlStatements.SELECT_JOB_TRANSFER_INFO;
              
              // Prepare the statement and fill in the placeholders.
              PreparedStatement pstmt = conn.prepareStatement(sql);
              pstmt.setString(1, jobUuid);
                          
              // Issue the call for the 1 row result set.
              ResultSet rs = pstmt.executeQuery();
              if (rs != null && rs.next()) {
                  result = new JobTransferInfo();
                  result.inputTransactionId   = rs.getString(1);
                  result.inputCorrelationId   = rs.getString(2);
                  result.archiveTransactionId = rs.getString(3);
                  result.archiveCorrelationId = rs.getString(4);
              }
              
              // Close the result and statement.
              rs.close();
              pstmt.close();
        
              // Commit the transaction.
              conn.commit();
          }
          catch (Exception e)
          {
              // Rollback transaction.
              try {if (conn != null) conn.rollback();}
                  catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
              
              String msg = MsgUtils.getMsg("DB_SELECT_UUID_ERROR", "Jobs", jobUuid, e.getMessage());
              throw new JobException(msg, e);
          }
          finally {
              // Always return the connection back to the connection pool.
              try {if (conn != null) conn.close();}
                catch (Exception e) 
                {
                  // If commit worked, we can swallow the exception.  
                  // If not, the commit exception will be thrown.
                  String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                  _log.error(msg, e);
                }
          }
	    
        // Make sure we found a job.
        if (result == null) {
            String msg = MsgUtils.getMsg("JOBS_JOB_NOT_FOUND", jobUuid);
            throw new TapisNotFoundException(msg, jobUuid);
        }
        
        // Non-null result.
	    return result;
	}
	
    /* ---------------------------------------------------------------------- */
    /* incrementRemoteStatusCheck:                                            */
    /* ---------------------------------------------------------------------- */
    public void incrementRemoteStatusCheck(Job job, boolean success)
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (job == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "incrementRemoteStatusCheck", "job");
            throw new JobException(msg);
        }
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Insert into the jobs table first.
          // Create the command using table definition field order.
          String sql;
          if (success) sql = SqlStatements.UPDATE_SUCCESS_STATUS_CHECKS;
            else sql = SqlStatements.UPDATE_FAILED_STATUS_CHECKS;
          Instant now = Instant.now();
          
          // Prepare the chosen statement.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setInt(1, 1);
          pstmt.setTimestamp(2, Timestamp.from(now));
          pstmt.setTimestamp(3, Timestamp.from(now));
          pstmt.setInt(4, job.getId());
          
          // Issue the call and check that one record was updated.
          int rows = pstmt.executeUpdate();
          if (rows != 1) {
              String parms = StringUtils.joinWith(", ", 1, now, now, job.getId());
              String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
              throw new JobException(msg);
          }
             
          // Close the result and statement.
          pstmt.close();
        
          // Commit the transaction.
          conn.commit();
          
          // Update the in-memory job with the latest information.
          if (success) job.setRemoteChecksSuccess(job.getRemoteChecksSuccess() + 1);
            else job.setRemoteChecksFailed(job.getRemoteChecksFailed() + 1);
          job.setLastUpdated(now);
          job.setRemoteLastStatusCheck(now);
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_UPDATE_ERROR", job.getId(), 
                                         job.getTenant(), job.getOwner(), e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
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
    }
    
    /* ---------------------------------------------------------------------- */
    /* setRemoteJobId:                                                        */
    /* ---------------------------------------------------------------------- */
    public void setRemoteJobId(Job job, String remoteId) throws JobException
    {setRemoteJobId(job, remoteId, true);}
    
    /* ---------------------------------------------------------------------- */
    /* setRemoteJobId2:                                                       */
    /* ---------------------------------------------------------------------- */
    public void setRemoteJobId2(Job job, String remoteId2) throws JobException
    {setRemoteJobId(job, remoteId2, false);}
    
    /* ---------------------------------------------------------------------- */
    /* setRemoteOutcome:                                                      */
    /* ---------------------------------------------------------------------- */
    public void setRemoteOutcome(Job job, JobRemoteOutcome outcome) 
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (job == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setRemoteOutcome", "job");
            throw new JobException(msg);
        }
        if (outcome == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setRemoteOutcome", "outcome");
            throw new JobException(msg);
        }
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Set the sql command.
          String sql = SqlStatements.UPDATE_REMOTE_OUTCOME;
          
          // Calculate the new values.
          Instant now = Instant.now();
          Timestamp ts = Timestamp.from(now);
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, outcome.name());
          pstmt.setTimestamp(2, ts);
          pstmt.setTimestamp(3, ts);
          pstmt.setLong(4, job.getId());
          
          // Issue the call.
          int rows = pstmt.executeUpdate();
          if (rows != 1) {
              String parms = StringUtils.joinWith(", ", outcome.name(), ts, ts, job.getId());
              String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
              _log.error(msg);
              throw new JobException(msg);
          }
          
          // Close the result and statement.
          pstmt.close();
        
          // Commit the transaction.
          conn.commit();
          
          // Update the job object.
          job.setRemoteOutcome(outcome);
          job.setLastUpdated(now);
          job.setRemoteEnded(now);
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_UPDATE_ERROR", job.getUuid(), 
                                         job.getTenant(), job.getOwner(), e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
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
    }

    /* ---------------------------------------------------------------------- */
    /* setRemoteOutcomeAndResult:                                             */
    /* ---------------------------------------------------------------------- */
    public void setRemoteOutcomeAndResult(Job job, JobRemoteOutcome outcome, String result) 
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (job == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setRemoteOutcome", "job");
            throw new JobException(msg);
        }
        if (outcome == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setRemoteOutcome", "outcome");
            throw new JobException(msg);
        }
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Set the sql command.
          String sql = SqlStatements.UPDATE_REMOTE_OUTCOME_AND_RESULT;
          
          // Calculate the new values.
          Instant now = Instant.now();
          Timestamp ts = Timestamp.from(now);
          
          // Prepare the statement and fill in the placeholders.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setString(1, outcome.name());
          pstmt.setString(2, result);
          pstmt.setTimestamp(3, ts);
          pstmt.setTimestamp(4, ts);
          pstmt.setLong(5, job.getId());
          
          // Issue the call.
          int rows = pstmt.executeUpdate();
          if (rows != 1) {
              String parms = StringUtils.joinWith(", ", outcome.name(), result, ts, ts, job.getId());
              String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
              _log.error(msg);
              throw new JobException(msg);
          }
          
          // Close the result and statement.
          pstmt.close();
        
          // Commit the transaction.
          conn.commit();
          
          // Update the job object.
          job.setRemoteOutcome(outcome);
          job.setRemoteResultInfo(result);
          job.setLastUpdated(now);
          job.setRemoteEnded(now);
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_UPDATE_ERROR", job.getUuid(), 
                                         job.getTenant(), job.getOwner(), e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
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
    }

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* setStatus:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Set the status of the specified job after checking that the transition
     * from the current status to the new status is legal.  If the commit flag
     * is true, then the transaction is committed and null is returned.  If the
     * commit flag is false, then the transaction is left uncommitted and the 
     * open connection is returned.  It is the caller's responsibility to commit
     * the transaction and close the connection after issuing any number of 
     * other database calls in the same transaction. 
     * 
     * The in-memory job object is also updated with all changes made to the 
     * database by this method and any method called from this method.
     * 
     * It is the responsibility of the caller or a method earlier in the call 
     * chain to create and process job events.  This method only affects the 
     * jobs table and in-memory job object.
     * 
     * @param uuid the job whose status is to change    
     * @param newStatus the job's new status
     * @param message the status message to be saved in the job record
     * @param commit true to commit the transaction and close the connection;
     *               false to leave the transaction and connection open
     * @param updateTime a specific instant for the last update time or null
     * @return the open connection when the transaction is uncommitted; null otherwise
     * @throws JobException if the status could not be updated
     */
    private Connection setStatus(Job job, JobStatusType newStatus, String message,
                                 boolean commit, Instant updateTime)
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (job == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setStatus", "job");
            throw new JobException(msg);
        }
        
        // Assign the update time if the caller hasn't.
        if (updateTime == null) updateTime = Instant.now();
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
            // Get a database connection.
            conn = getConnection();
            
            // --------- Get current status
            // Get the current job status from the database and keep record locked.
            String sql = SqlStatements.SELECT_JOB_STATUS_FOR_UPDATE;
       
            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, job.getTenant());
            pstmt.setString(2, job.getUuid());
            
            // Issue the call for the 1 row, 1 field result set.
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) {
                String msg = MsgUtils.getMsg("DB_SELECT_EMPTY_RESULT", sql, 
                                             StringUtils.joinWith(", ", job.getTenant(), job.getUuid()));
                throw new JobException(msg);
            }
                
            // Get current status.
            String curStatusString = rs.getString(1);
            JobStatusType curStatus = JobStatusType.valueOf(curStatusString);
            
            // Debug logging.
            if (_log.isDebugEnabled())
                _log.debug(MsgUtils.getMsg("JOBS_STATUS_UPDATE", job.getUuid(), 
                                           curStatusString, newStatus.name()));

            // --------- Validate requested status transition ---------
            if (!JobFSMUtils.hasTransition(curStatus, newStatus)) {
                String msg = MsgUtils.getMsg("JOBS_STATE_NO_TRANSITION", job.getUuid(), 
                                             curStatusString, newStatus.name());
                throw new JobException(msg);
            }
            // --------------------------------------------------------
            
            // Truncate message if it's longer than the database field length.
            if (message.length() > Job.MAX_LAST_MESSAGE_LEN) 
               message = message.substring(0, Job.MAX_LAST_MESSAGE_LEN - 1);
            
            // Increment the blocked counter if we are transitioning to the blocked state.
            int blockedIncrement = 0;
            if (newStatus == JobStatusType.BLOCKED && curStatus != JobStatusType.BLOCKED)
                blockedIncrement = 1;
            
            // --------- Set new status
            sql = SqlStatements.UPDATE_JOB_STATUS;
            Timestamp ts = Timestamp.from(updateTime);
            
            // Prepare the statement and fill in the placeholders.
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, newStatus.name());
            pstmt.setString(2, message);
            pstmt.setTimestamp(3, ts);
            pstmt.setInt(4, blockedIncrement);
            pstmt.setString(5, job.getTenant());
            pstmt.setString(6, job.getUuid());
            
            // Issue the call.
            int rows = pstmt.executeUpdate();
            if (rows != 1) {
                String parms = StringUtils.joinWith(", ", newStatus.name(), message, ts, 
                                                    blockedIncrement, job.getTenant(), job.getUuid());
                String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
                _log.error(msg);
                throw new JobException(msg);
            }
            
            // Set the remote execution start time when the new status transitions to RUNNING
            // or the job ended time if we have transitioned to a terminal state. The called
            // methods also update the in-memory job object.
            if (newStatus == JobStatusType.RUNNING) updateRemoteStarted(conn, job, ts);
            else if (newStatus.isTerminal()) updateEnded(conn, job, ts);
            
            // Write the event table and optionally send notifications (asynchronously).
            var eventMgr = JobEventManager.getInstance();
            eventMgr.recordStatusEvent(job, newStatus, curStatus, conn);
            
            // Conditionally commit the transaction.
            if (commit) conn.commit();
            
            // Update the in-memory job object.
            job.setStatus(newStatus);
            job.setLastMessage(message);
            job.setLastUpdated(updateTime);
            job.setBlockedCount(job.getBlockedCount() + blockedIncrement);
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            // Close and null out the connection here. This overrides the finally block logic and
            // guarantees that we will not interfere with another thread's use of the connection. 
            try {if (conn != null) conn.close(); conn = null;}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE"), e1);}
            
            String msg = MsgUtils.getMsg("JOBS_JOB_SELECT_UUID_ERROR", job.getUuid(), 
                                         job.getTenant(), job.getOwner(), e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Conditionally return the connection back to the connection pool.
            if (commit && (conn != null)) 
                try {conn.close();}
                  catch (Exception e) 
                  {
                      // If commit worked, we can swallow the exception.  
                      // If not, the commit exception will be thrown.
                      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
                      _log.error(msg, e);
                  }
        }
        
        // Return the open connection when no commit occurred.
        if (commit) return null;
          else return conn;
    }

    /* ---------------------------------------------------------------------- */
    /* updateRemoteStarted:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Set the remote started timestamp to be equal to the specified timestamp
     * only if the remote started timestamp is null.  This method is really just 
     * an extension of the setStatus() method separated for readability.  
     * 
     * Once set, the remote started timestamp is not updated by this method, so 
     * calling it more than once for a job will not change the job record.
     * 
     * @param conn the connection with the in-progress transaction
     * @param uuid the job uuid
     * @param ts the remote execution start time
     * @throws SQLException
     */
    private void updateRemoteStarted(Connection conn, Job job, Timestamp ts) 
     throws SQLException
    {
        // Set the sql command.
        String sql = SqlStatements.UPDATE_REMOTE_STARTED;
            
        // Prepare the statement and fill in the placeholders.
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setTimestamp(1, ts);
        pstmt.setString(2, job.getUuid());
            
        // Issue the call.
        int rows = pstmt.executeUpdate();
        
        // Update the in-memory object.
        job.setRemoteStarted(ts.toInstant());
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateEnded:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Set the ended timestamp to be equal to the specified timestamp
     * only if the ended timestamp is null.  This method is really just 
     * an extension of the setStatus() method separated for readability.  
     * 
     * Once set, the ended timestamp is not updated by this method, so 
     * calling it more than once for a job will not change the job record.
     * 
     * @param conn the connection with the in-progress transaction
     * @param uuid the job uuid
     * @param ts the job termination time
     * @throws SQLException
     */
    private void updateEnded(Connection conn, Job job, Timestamp ts) 
     throws SQLException
    {
        // Set the sql command.
        String sql = SqlStatements.UPDATE_JOB_ENDED;
            
        // Prepare the statement and fill in the placeholders.
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setTimestamp(1, ts);
        pstmt.setString(2, job.getUuid());
            
        // Issue the call.
        int rows = pstmt.executeUpdate();
        
        // Update the in-memory object.
        job.setEnded(ts.toInstant());
    }
    
    /* ---------------------------------------------------------------------- */
    /* countActiveSystemJobs:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Retrieve the number of jobs in active state on the specified execution
     * system with optional owner and remote queue filtering.
     * 
     * @param tenantId the non-null execution system's tenant id
     * @param systemId the non-null execution system's unique id
     * @param owner job owner or null for any owner
     * @param logicalQueue remote queue or null for any queue
     * @param pendingActive true means Pending is considered an active state, false means inactive
     * @return the number of tapis jobs active on the specified system
     * @throws JobException 
     */
    private int countActiveJobs(String tenantId, String systemId, String owner, 
                                String logicalQueue, boolean pendingActive) 
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (StringUtils.isBlank(tenantId)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "countActiveJobs", "tenantId");
            _log.error(msg);
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(systemId)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "countActiveJobs", "systemId");
            _log.error(msg);
            throw new JobException(msg);
        }
        
        // Select the query case based on the owner and logicalQueue values.  
        int queryCase;
        String sql;
        if (owner == null && logicalQueue == null) {
            queryCase = 1;
            sql = SqlStatements.COUNT_ACTIVE_SYSTEM_JOBS;
        } else if (owner != null && logicalQueue == null) {
            queryCase = 2;
            sql = SqlStatements.COUNT_ACTIVE_SYSTEM_USER_JOBS;
        } else if (owner == null && logicalQueue != null) {
            queryCase = 3;
            sql = SqlStatements.COUNT_ACTIVE_SYSTEM_QUEUE_JOBS;
        } else {
            queryCase = 4;
            sql = SqlStatements.COUNT_ACTIVE_SYSTEM_USER_QUEUE_JOBS;
        }

        // Substitute the comma-separated non-active 
        // status list for the placeholder text.
        String nonActiveJobStatuses = 
            pendingActive ? _nonActiveWithoutPendingJobStatuses : _nonActiveWithPendingJobStatuses;
        sql = sql.replace(":statusList", nonActiveJobStatuses);
        
        // The result.
        int count = 0;
        
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
            // Get a database connection.
            conn = getConnection();
            
            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tenantId);
            pstmt.setString(2, systemId);
            
            // Conditional value assignments.
            if (queryCase == 1) {/* do nothing */}
            else if (queryCase == 2) pstmt.setString(3, owner);
            else if (queryCase == 3) pstmt.setString(3, logicalQueue);
            else if (queryCase == 4) {
                pstmt.setString(3, owner);
                pstmt.setString(4, logicalQueue);
            }
                        
            // Issue the call for the 1 row result set.
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) count = rs.getInt(1);
            
            // Close the result and statement.
            rs.close();
            pstmt.close();
      
            // Commit the transaction.
            conn.commit();
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String ownerMsg = owner == null ? "*" : owner;
            String logicalQueueMsg = logicalQueue == null ? "*" : logicalQueue;
            String msg = MsgUtils.getMsg("JOBS_COUNT_ACTIVE_SYSTEM_JOBS", tenantId, systemId, 
                                         ownerMsg, logicalQueueMsg, e.getMessage());
            _log.error(msg, e);
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
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
        
        return count;
    }

    /* ---------------------------------------------------------------------- */
    /* setRemoteJobId:                                                        */
    /* ---------------------------------------------------------------------- */
    private void setRemoteJobId(Job job, String remoteId, boolean primary) 
     throws JobException
    {
        // ------------------------- Check Input -------------------------
        if (job == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setRemoteJobId", "job");
            throw new JobException(msg);
        }
        if (StringUtils.isBlank(remoteId)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "setRemoteJobId", "remoteId");
            throw new JobException(msg);
        }
        
        // Get current time.
        var now = Instant.now();

        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
          // Get a database connection.
          conn = getConnection();

          // Insert into the jobs table first.
          // Create the command using table definition field order.
          String sql = primary ? SqlStatements.UPDATE_REMOTE_JOB_ID : SqlStatements.UPDATE_REMOTE_JOB_ID2;
          
          // Prepare the chosen statement.
          PreparedStatement pstmt = conn.prepareStatement(sql);
          pstmt.setTimestamp(1, Timestamp.from(now));
          pstmt.setString(2, remoteId);
          pstmt.setInt(3, job.getId());
          pstmt.setString(4, job.getTenant());
          
          // Issue the call and check that one record was updated.
          int rows = pstmt.executeUpdate();
          if (rows != 1) {
              String parms = StringUtils.joinWith(", ", now, remoteId, job.getId(), job.getTenant());
              String msg = MsgUtils.getMsg("DB_UPDATE_UNEXPECTED_ROWS", 1, rows, sql, parms);
              throw new JobException(msg);
          }
             
          // Close the result and statement.
          pstmt.close();
        
          // Commit the transaction.
          conn.commit();
          
          // Update the in-memory job with the latest information.
          job.setLastUpdated(now);
          if (primary) job.setRemoteJobId(remoteId);
            else job.setRemoteJobId2(remoteId);
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            String idSuffix = primary ? "" : "2";
            String msg = MsgUtils.getMsg("JOBS_UPDATE_REMOTE_ID_ERROR", job.getUuid(), 
                                         job.getTenant(), job.getOwner(), 
                                         idSuffix, remoteId, e.getMessage());
            throw new JobException(msg, e);
        }
        finally {
            // Always return the connection back to the connection pool.
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
    }

	/* ---------------------------------------------------------------------- */
	/* validateNewJob:                                                        */
	/* ---------------------------------------------------------------------- */
	private void validateNewJob(Job job) throws TapisException
	{
		// Check each field used to create a new job.
		if (StringUtils.isBlank(job.getName())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "name");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getOwner())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "owner");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getTenant())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "tenant");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getDescription())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "description");
	          throw new TapisException(msg);
		}
		
		if (job.getStatus() == null) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "status");
	          throw new TapisException(msg);
		}
		if (job.getJobType() == null) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "jobType");
            throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getUuid())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "uuid");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getAppId())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "appId");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getAppVersion())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "appVersion");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getExecSystemId())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemId");
	          throw new TapisException(msg);
		}
		
        if (StringUtils.isBlank(job.getExecSystemExecDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemExecDir");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getExecSystemInputDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemInputDir");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getExecSystemOutputDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "execSystemOutputDir");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getArchiveSystemId())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "archiveSystemId");
            throw new TapisException(msg);
        }
      
        if (StringUtils.isBlank(job.getArchiveSystemDir())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "archiveSystemDir");
            throw new TapisException(msg);
        }
      
		// For flexibility, we allow the hpc scheduler to deal with validating resource reservation values.
		if (job.getMaxMinutes() < 1) {
	          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validateNewJob", "maxMinutes", job.getMaxMinutes());
	          throw new TapisException(msg);
		}

		if (StringUtils.isBlank(job.getFileInputs())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "fileInputs");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getParameterSet())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "parameterSet");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getSubscriptions())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "notifications");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getTapisQueue())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "tapisQueue");
	          throw new TapisException(msg);
		}
		
		if (StringUtils.isBlank(job.getCreatedby())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "createdBy");
	          throw new TapisException(msg);
		}
		if (StringUtils.isBlank(job.getCreatedbyTenant())) {
	          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateNewJob", "createdByTenant");
	          throw new TapisException(msg);
		}
	}
	
	/* ---------------------------------------------------------------------- */
	/* populateJob:                                                           */
	/* ---------------------------------------------------------------------- */
	/** Populate a new Jobs object with a record retrieved from the 
	 * database.  The result set's cursor will be advanced to the next
	 * position and, if a row exists, its data will be marshalled into a 
	 * Jobs object.  The result set is not closed by this method.
	 * 
	 * NOTE: This method assumes all fields are returned table definition order.
	 * 
	 * NOTE: This method must be manually maintained whenever the table schema changes.  
	 * 
	 * @param rs the unprocessed result set from a query.
	 * @return a new model object or null if the result set is null or empty
	 * @throws TapisJDBCException on SQL access or conversion errors
	 */
	private Job populateJob(ResultSet rs)
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
	      throw new TapisJDBCException(msg, e);
	    }
	    
	    // Populate the Jobs object using table definition field order,
	    // which is the order specified in all calling methods.
	    Job obj = new Job();
	    try {
	        obj.setId(rs.getInt(1));
	        obj.setName(rs.getString(2));
	        obj.setOwner(rs.getString(3));
	        obj.setTenant(rs.getString(4));
	        obj.setDescription(rs.getString(5));
	        obj.setStatus(JobStatusType.valueOf(rs.getString(6)));
	        obj.setLastMessage(rs.getString(7));
	        obj.setCreated(rs.getTimestamp(8).toInstant());

	        Timestamp ts = rs.getTimestamp(9);
	        if (ts != null) obj.setEnded(ts.toInstant());

	        obj.setLastUpdated(rs.getTimestamp(10).toInstant());
	        obj.setUuid(rs.getString(11));
	        obj.setAppId(rs.getString(12));
	        obj.setAppVersion(rs.getString(13));
	        obj.setArchiveOnAppError(rs.getBoolean(14));
	        obj.setDynamicExecSystem(rs.getBoolean(15));
	        
	        obj.setExecSystemId(rs.getString(16));
	        obj.setExecSystemExecDir(rs.getString(17));
	        obj.setExecSystemInputDir(rs.getString(18));
	        obj.setExecSystemOutputDir(rs.getString(19));
	        obj.setExecSystemLogicalQueue(rs.getString(20));
	        
	        obj.setArchiveSystemId(rs.getString(21));
	        obj.setArchiveSystemDir(rs.getString(22));
	        
	        obj.setDtnSystemId(rs.getString(23));
	        obj.setDtnMountSourcePath(rs.getString(24));
	        obj.setDtnMountPoint(rs.getString(25));
	        
	        obj.setNodeCount(rs.getInt(26));
	        obj.setCoresPerNode(rs.getInt(27));
	        obj.setMemoryMB(rs.getInt(28));
	        obj.setMaxMinutes(rs.getInt(29));
	        
	        obj.setFileInputs(rs.getString(30));
	        obj.setParameterSet(rs.getString(31));
	        obj.setExecSystemConstraints(rs.getString(32));
	        obj.setSubscriptions(rs.getString(33));	        
	        
	        obj.setBlockedCount(rs.getInt(34));
	        obj.setRemoteJobId(rs.getString(35));
	        obj.setRemoteJobId2(rs.getString(36));
	        
	        String s = rs.getString(37);
	        if (s != null) obj.setRemoteOutcome(JobRemoteOutcome.valueOf(s));
	        obj.setRemoteResultInfo(rs.getString(38));
	        obj.setRemoteQueue(rs.getString(39));

	        ts = rs.getTimestamp(40);
	        if (ts != null) obj.setRemoteSubmitted(ts.toInstant());

	        ts = rs.getTimestamp(41);
	        if (ts != null) obj.setRemoteStarted(ts.toInstant());

	        ts = rs.getTimestamp(42);
	        if (ts != null) obj.setRemoteEnded(ts.toInstant());

	        obj.setRemoteSubmitRetries(rs.getInt(43));
	        obj.setRemoteChecksSuccess(rs.getInt(44));
	        obj.setRemoteChecksFailed(rs.getInt(45));

	        ts = rs.getTimestamp(46);
	        if (ts != null) obj.setRemoteLastStatusCheck(ts.toInstant());
	        
	        obj.setInputTransactionId(rs.getString(47));
	        obj.setInputCorrelationId(rs.getString(48));
	        obj.setArchiveTransactionId(rs.getString(49));
	        obj.setArchiveCorrelationId(rs.getString(50));

	        obj.setTapisQueue(rs.getString(51));
	        obj.setVisible(rs.getBoolean(52));
	        obj.setCreatedby(rs.getString(53));
	        obj.setCreatedbyTenant(rs.getString(54));
	        
	        Array tagsArray = rs.getArray(55);
	        if (tagsArray != null) {
	            var stringArray = (String[])tagsArray.getArray();
	            if (stringArray != null && stringArray.length > 0) { 
	                var tagsSet = new TreeSet<String>();
	                for (String s1 : stringArray) tagsSet.add(s1);
	                obj.setTags(tagsSet);
	            }
	        }
	        
	        // Could be null until databases are migrated.
	        String jobType = rs.getString(56);
	        if (jobType != null) obj.setJobType(JobType.valueOf(jobType));
	        
	        // MPI and command prefix.
	        obj.setMpi(rs.getBoolean(57));
	        obj.setMpiCmd(rs.getString(58));
	        obj.setCmdPrefix(rs.getString(59));
	        
	        // Shared application context.
	        obj.setSharedAppCtx(rs.getString(60));
	        Array attribArray = rs.getArray(61);
	        if (attribArray != null) {
	            var stringArray = (String[])attribArray.getArray();
                if (stringArray != null && stringArray.length > 0) { 
                    var attribsList = new ArrayList<JobSharedAppCtxEnum>(6); // max number of elements
                    for (String s1 : stringArray) attribsList.add(JobSharedAppCtxEnum.valueOf(s1));
                    obj.setSharedAppCtxAttribs(attribsList);
                }
	        }
	        
	        // Notes non-null json value.
	        obj.setNotes(rs.getString(62));
	    } 
	    catch (Exception e) {
	      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
	      throw new TapisJDBCException(msg, e);
	    }
	      
	    return obj;
	}
	
	/* -------------------------------------------------- */
	/*              Search private methods                */
	/* -------------------------------------------------- */
	 private Condition addSearchListToWhere(Condition whereCondition, List<String> searchList) 
	 throws TapisException {
	    	if (searchList == null || searchList.isEmpty()) return whereCondition;
	        // Parse searchList and add conditions to the WHERE clause
	        for (String condStr : searchList)
	        {
	          whereCondition = addSearchCondStrToWhere(whereCondition, condStr, "AND");
	        }
	        return whereCondition;
		}

	 private Condition addSearchCondStrToWhere(Condition whereCondition, String searchStr, String joinOp) 
		throws TapisException {
	        // If we have no search string then return what we were given
		    if (StringUtils.isBlank(searchStr)) return whereCondition;
		    // If we are given a condition but no indication of how to join new condition to it then return what we were given
		    if (whereCondition != null && StringUtils.isBlank(joinOp)) return whereCondition;
		    if (whereCondition != null && joinOp != null && !joinOp.equalsIgnoreCase("AND") && !joinOp.equalsIgnoreCase("OR"))
		    {
		      return whereCondition;
		    }

		    // Parse search value into column name, operator and value
		    // Format must be column_name.op.value
		    String[] parsedStrArray = searchStr.split("\\.", 3);
		    // Validate column name
		    String column = parsedStrArray[0];
		    Field<?> col = Tables.JOBS.field(DSL.name(column));
		    // Check for column name passed in as camelcase
		    if (col == null)
		    {
		      col = Tables.JOBS.field(DSL.name(SearchUtils.camelCaseToSnakeCase(column)));
		    }
		    // If column not found then it is an error
		    if (col == null)
		    {
		      String msg = MsgUtils.getMsg("SEARCH_DB_NO_COLUMN", DSL.name(column));
		      throw new TapisException(msg);
		    }
		    // Validate and convert operator string
		    String opStr = parsedStrArray[1].toUpperCase();
		    SearchOperator op = SearchUtils.getSearchOperator(opStr);
		    if (op == null)
		    {
		      String msg = MsgUtils.getMsg("SEARCH_DB_INVALID_SEARCH_OP", opStr, Tables.JOBS.getName(), DSL.name(column));
		      throw new TapisException(msg);
		    }

		    // Check that column value is compatible for column type and search operator
		    String val = parsedStrArray[2];
		    checkConditionValidity(col, op, val);

		     // If val is a timestamp then convert the string(s) to a form suitable for SQL
		    // Use a utility method since val may be a single item or a list of items, e.g. for the BETWEEN operator
		    if (col.getDataType().getSQLType() == Types.TIMESTAMP)
		    {
		      val = SearchUtils.convertValuesToTimestamps(op, val);
		    }

		    // Create the condition
		    Condition newCondition = createCondition(col, op, val);
		    // If specified add the condition to the WHERE clause
		    if (StringUtils.isBlank(joinOp) || whereCondition == null) return newCondition;
		    else if (joinOp.equalsIgnoreCase("AND")) return whereCondition.and(newCondition);
		    else if (joinOp.equalsIgnoreCase("OR")) return whereCondition.or(newCondition);
		    return newCondition;
		}

	@SuppressWarnings("unchecked")
	private Condition createCondition(Field col, SearchOperator op, String val) {
		    boolean negateContains = true;
			List<String> valList = Collections.emptyList();
			SearchOperator op1 = op;
			if (SearchUtils.listOpSet.contains(op)) valList = SearchUtils.getValueList(val);
		    // If operator is IN or NIN and column type is array then handle it as CONTAINS or NCONTAINS
		    if ((col.getDataType().getSQLType() == Types.ARRAY) && SearchOperator.IN.equals(op)) op1 = CONTAINS;
		    if ((col.getDataType().getSQLType() == Types.ARRAY) && SearchOperator.NIN.equals(op)) op1 = NCONTAINS;
		    switch (op1) {
		      case EQ:
		        return col.eq(val);
		      case NEQ:
		        return col.ne(val);
		      case LT:
		        return col.lt(val);
		      case LTE:
		        return col.le(val);
		      case GT:
		        return col.gt(val);
		      case GTE:
		        return col.ge(val);
		      case LIKE:
		        return col.like(val);
		      case NLIKE:
		        return col.notLike(val);
		      case IN:
		        return col.in(valList);
		      case NIN:
		        return col.notIn(valList);
		      case CONTAINS:
		    	  negateContains = false;
		    	  return  textArrayOverlaps(col, valList.toArray(),negateContains );  
		      case NCONTAINS: 
		    	   return  textArrayOverlaps(col, valList.toArray(),negateContains);
		      case BETWEEN:
		        return col.between(valList.get(0), valList.get(1));
		      case NBETWEEN:
		        return col.notBetween(valList.get(0), valList.get(1));
		    }
			return null;
		}
	   
	  /*
	   * Implement the array overlap construct in jooq.
	   * Given a column as a Field<T[]> and a java array create a jooq condition that
	   * returns true if column contains any of the values in the array.
	   */
	  private static <T> Condition textArrayOverlaps(Field<T[]> col, T[] array, boolean negate)
	  {
		  Condition cond = DSL.condition("{0} && {1}::text[]", col, DSL.array(array));
		  if (negate) return cond.not();
		  else return cond;
	    
	  }
	  
	private void checkConditionValidity(Field<?> col, SearchOperator op, String valStr) 
	throws TapisException {
			var dataType = col.getDataType();
		    int sqlType = dataType.getSQLType();
		    String sqlTypeName = dataType.getTypeName();

		    // Make sure we support the sqlType
		    if (SearchUtils.ALLOWED_OPS_BY_TYPE.get(sqlType) == null)
		    {
		      String msg = MsgUtils.getMsg("SEARCH_DB_UNSUPPORTED_SQLTYPE", Tables.JOBS.getName(), col.getName(), op.name(), sqlTypeName);
		      throw new TapisException(msg);
		    }
		    // Check that operation is allowed for column data type
		    if (!SearchUtils.ALLOWED_OPS_BY_TYPE.get(sqlType).contains(op))
		    {
		      String msg = MsgUtils.getMsg("SEARCH_DB_INVALID_SEARCH_TYPE", Tables.JOBS.getName(), col.getName(), op.name(), sqlTypeName);
		      throw new TapisException(msg);
		    }

		    // Check that value (or values for op that takes a list) are compatible with sqlType
		    if (!SearchUtils.validateTypeAndValueList(sqlType, op, valStr, sqlTypeName, Tables.JOBS.getName(), col.getName()))
		    {
		      String msg = MsgUtils.getMsg("SEARCH_DB_INVALID_SEARCH_VALUE", op.name(), sqlTypeName, valStr, Tables.JOBS.getName(), col.getName());
		      throw new TapisException(msg);
		    }
		  }
			
     
		/**
		   * Create a condition for abstract syntax tree nodes by recursively walking the tree
		   * @param astNode Abstract syntax tree node to add to the base condition
		   * @return resulting condition
		   * @throws TapisException on error
		   */
		  private  Condition createConditionFromAst(ASTNode astNode) throws TapisException
		  {
		    if (astNode == null || astNode instanceof ASTLeaf)
		    {
		      // A leaf node is a column name or value. Nothing to process since we only process a complete condition
		      //   having the form column_name.op.value. We should never make it to here
		      String msg = "";//LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST1", (astNode == null ? "null" : astNode.toString()));
		      throw new TapisException(msg);
		    }
		    else if (astNode instanceof ASTUnaryExpression)
		    {
		      // A unary node should have no operator and contain a binary node with two leaf nodes.
		      // NOTE: Currently unary operators not supported. If support is provided for unary operators (such as NOT) then
		      //   changes will be needed here.
		      ASTUnaryExpression unaryNode = (ASTUnaryExpression) astNode;
		      if (!StringUtils.isBlank(unaryNode.getOp()))
		      {
		        String msg = "";//LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_UNARY_OP", unaryNode.getOp(), unaryNode.toString());
		        throw new TapisException(msg);
		      }
		      // Recursive call
		      return createConditionFromAst(unaryNode.getNode());
		    }
		    else if (astNode instanceof ASTBinaryExpression)
		    {
		      // It is a binary node
		      ASTBinaryExpression binaryNode = (ASTBinaryExpression) astNode;
		      // Recursive call
		      return createConditionFromBinaryExpression(binaryNode);
		    }
		    return null;
		  }
		  
		  /**
		   * Create a condition from an abstract syntax tree binary node
		   * @param binaryNode Abstract syntax tree binary node to add to the base condition
		   * @return resulting condition
		   * @throws TapisException on error
		   */
		  private  Condition createConditionFromBinaryExpression(ASTBinaryExpression binaryNode) throws TapisException
		  {
		    // If we are given a null then something went very wrong.
		    if (binaryNode == null)
		    {
		      throw new TapisException(""/*LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST2")*/);
		    }
		    // If operator is AND or OR then make recursive call for each side and join together
		    // For other operators build the condition left.op.right and add it
		    String op = binaryNode.getOp();
		    ASTNode leftNode = binaryNode.getLeft();
		    ASTNode rightNode = binaryNode.getRight();
		    if (StringUtils.isBlank(op))
		    {
		      throw new TapisException(""/*LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST3", binaryNode.toString())*/);
		    }
		    else if (op.equalsIgnoreCase("AND"))
		    {
		      // Recursive calls
		      Condition cond1 = createConditionFromAst(leftNode);
		      Condition cond2 = createConditionFromAst(rightNode);
		      if (cond1 == null || cond2 == null)
		      {
		        throw new TapisException(""/*LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST4", binaryNode.toString())*/);
		      }
		      return cond1.and(cond2);

		    }
		    else if (op.equalsIgnoreCase("OR"))
		    {
		      // Recursive calls
		      Condition cond1 = createConditionFromAst(leftNode);
		      Condition cond2 = createConditionFromAst(rightNode);
		      if (cond1 == null || cond2 == null)
		      {
		        throw new TapisException(""/*LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST4", binaryNode.toString())*/);
		      }
		      return cond1.or(cond2);

		    }
		    else
		    {
		      // End of recursion. Create a single condition.
		      // Since operator is not an AND or an OR we should have 2 unary nodes or a unary and leaf node
		      String lValue;
		      String rValue;
		      if (leftNode instanceof ASTLeaf) lValue = ((ASTLeaf) leftNode).getValue();
		      else if (leftNode instanceof ASTUnaryExpression) lValue =  ((ASTLeaf) ((ASTUnaryExpression) leftNode).getNode()).getValue();
		      else
		      {
		        throw new TapisException(""/*LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST5", binaryNode.toString())*/);
		      }
		      if (rightNode instanceof ASTLeaf) rValue = ((ASTLeaf) rightNode).getValue();
		      else if (rightNode instanceof ASTUnaryExpression) rValue =  ((ASTLeaf) ((ASTUnaryExpression) rightNode).getNode()).getValue();
		      else
		      {
		        throw new TapisException(""/*LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST6", binaryNode.toString())*/);
		      }
		      // Build the string for the search condition, left.op.right
		      String condStr = String.format("%s.%s.%s", lValue, binaryNode.getOp(), rValue);
		      // Validate and create a condition from the string
		      return addSearchCondStrToWhere(null, condStr, null);
		    }
		  }
		  
    /* ---------------------------------------------------------------------- */
    /* getDBJobColumnAndType:                                                 */
    /* ---------------------------------------------------------------------- */
    /**
     * getDBJobColumnAndType: Get resource model's DB table columns
     * @param tableName
     * @return
     */
    public static  Map<String, String> getDBJobColumnAndType(String tableName) {
        
    	Map<String,String> jmap = new HashMap<String,String>(80);
     
        // ------------------------- Call SQL ----------------------------
        Connection conn = null;
        try
        {
            // Get a database connection.
        	DataSource ds = getDataSource();
	        conn = ds.getConnection();
            
            // Get the select command.
	        // it queries column name and type
            String sql = SqlStatements.SELECT_COLUMN_DATA_TYPE_BY_TABLENAME;
            sql = sql.replace(":tablename", tableName);
            // Prepare the statement and fill in the placeholders.
            PreparedStatement pstmt = conn.prepareStatement(sql);
            _log.debug("prepared stmt: " + pstmt);
                        
            // Issue the call for the 1 row result set.
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) {
                String msg = MsgUtils.getMsg("SEARCH_ABORT_UNABLE_TO_GET_DB_FIELD_AND_TYPE", tableName);
                _log.error(msg);
                throw new JobException(msg);
            }
            
            // Extract the field name and type from the result set.
            do {
                jmap.put(rs.getString(1), rs.getString(2));
                
                // This can be uncommented to check if it is returning all column names and type
                //_log.debug("key = " + rs.getString(1) + "  type= "+ rs.getString(2) );
            } while(rs.next()) ; 
           
                     
            // Close the result and statement.
            rs.close();
            pstmt.close();
      
            // Commit the transaction.
            conn.commit();
        }
        catch (Exception e)
        {
            // Rollback transaction.
            try {if (conn != null) conn.rollback();}
                catch (Exception e1){_log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);}
            
            //AloeThreadContext threadContext = AloeThreadLocal.aloeThreadContext.get();
            String msg = MsgUtils.getMsg("DB_TABLE_INFORMATION_SCHEMA_ERROR");
            _log.error(msg, e);
           
        }
        finally {
            // Always return the connection back to the connection pool.
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
        return jmap;
        
    }
    
    /* ************************************* */
    /*            Initialize Jobs Map        */
    /* ************************************* */ 
    public static Map<String,String> initializeJobFieldMap(){
        // Map<String,String> jmap = new HashMap<String,String>(80);
        Map<String, String> jmap;
        jmap = getDBJobColumnAndType(JOBS_TABLENAME);
                
        return Collections.unmodifiableMap(jmap);
    }
    
    /* ********************************************************************** */
    /*                          JobTransferInfo class                         */
    /* ********************************************************************** */
    // Container for file transfer information.
    public static final class JobTransferInfo
    {
        public String inputTransactionId;
        public String inputCorrelationId;
        public String archiveTransactionId;
        public String archiveCorrelationId;
    }

    /* ********************************************************************** */
    /*                           JobNameOwner class                           */
    /* ********************************************************************** */
    // Container for job name and owner information.
    public static final class JobNameOwner
    {
        public String name;
        public String owner;
    }
}
    
