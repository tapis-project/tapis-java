package edu.utexas.tacc.tapis.jobs.api.resources;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonSyntaxException;

import edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearch;
import edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearchAllAttributes;
import edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearchSelectAttributes;
import edu.utexas.tacc.tapis.jobs.api.utils.JobListUtils;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.dto.JobListDTO;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobListType;
import edu.utexas.tacc.tapis.jobs.utils.SelectTuple;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

@Path("/search")
public class JobSearchResource extends AbstractResource {
	
	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(JobSearchResource.class);
    
    
    // Json schema resource files
    
    private static final String FILE_JOB_SEARCH_REQUEST = "/edu/utexas/tacc/tapis/jobs/api/jsonschema/JobSearchRequest.json";
    private static final int DEFAULT_TOTAL_COUNT = -1;
    private final String UUID_ATTR = "uuid";
    private final String SEARCH_IN_OPERATOR = "IN";
    private final String AND_OPERATOR = "AND";
    private final String SPACE = " ";
    private final String COMMA = ",";
    private final String LEFT_PARENTHESIS = "(";
    private final String RIGHT_PARENTHESIS = ")";
    private final String QUOTE = "'";
    private final boolean SHARED = true;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    /* Jax-RS context dependency injection allows implementations of these abstract
     * types to be injected (ch 9, jax-rs 2.0):
     * 
     *      javax.ws.rs.container.ResourceContext
     *      javax.ws.rs.core.Application
     *      javax.ws.rs.core.HttpHeaders
     *      javax.ws.rs.core.Request
     *      javax.ws.rs.core.SecurityContext
     *      javax.ws.rs.core.UriInfo
     *      javax.ws.rs.core.Configuration
     *      javax.ws.rs.ext.Providers
     * 
     * In a servlet environment, Jersey context dependency injection can also 
     * initialize these concrete types (ch 3.6, jersey spec):
     * 
     *      javax.servlet.HttpServletRequest
     *      javax.servlet.HttpServletResponse
     *      javax.servlet.ServletConfig
     *      javax.servlet.ServletContext
     *
     * Inject takes place after constructor invocation, so fields initialized in this
     * way can not be accessed in constructors.
     */ 
     @Context
     private HttpHeaders        _httpHeaders;
  
     @Context
     private Application        _application;
  
     @Context
     private UriInfo            _uriInfo;
  
     @Context
     private SecurityContext    _securityContext;
  
     @Context
     private ServletContext     _servletContext;
  
     @Context
     private HttpServletRequest _request;
     
     /* **************************************************************************** */
     /*                                Public Methods                                */
     /* **************************************************************************** */
     
	/* ---------------------------------------------------------------------------- */
    /* getJobSearchList:                                                            */
    /* ---------------------------------------------------------------------------- */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Retrieve list of jobs for the user based on search conditions in the query paramter on the dedicated search end-point.\n\n"
                          + "The caller must be the job owner, creator or a tenant administrator. \n\n"
                          + "List of Jobs shared with the user can also be searched",
            tags = "jobs",
            security = {@SecurityRequirement(name = "TapisJWT")},
            responses = 
                {
                 @ApiResponse(responseCode = "200", description = "Jobs Search List retrieved.",
                     content = @Content(schema = @Schema(
                   		  implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearchAllAttributes.class))),
                 @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "403", description = "Forbidden.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "404", description = "Jobs not found.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                 @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
    )
    public Response getJobSearchList(
   		 		@QueryParam("limit") int limit, 
   		 		@QueryParam("skip") int skip,
   		 		@QueryParam("startAfter") int startAfter,
   		 		@QueryParam("orderBy") String orderBy,
   		 		@QueryParam("computeTotal") boolean computeTotal,
   		 		@QueryParam("select") String select,
   		 	    @DefaultValue("MY_JOBS") @QueryParam("listType") String listType,
                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                              
    {
      // Trace this request.
      if (_log.isTraceEnabled()) {
        String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getJobSearchList", 
                                     "  " + _request.getRequestURL());
        _log.trace(msg);
      }
      
      // ------------------------- Create Context ---------------------------
      // Validate the threadlocal content here so no subsequent code on this request needs to.
      TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
      if (!threadContext.validate()) {
          var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
          _log.error(msg);
          return Response.status(Status.INTERNAL_SERVER_ERROR).
                  entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      
      // ---------------------- Get the Search Query Parameters --------------------
      int totalCount = DEFAULT_TOTAL_COUNT;
      List<String> searchList;
      List<String> sharedSearchList = new ArrayList<String>();
      
      try
      {
        searchList = SearchUtils.buildListFromQueryParms(_uriInfo.getQueryParameters());
      }
      catch (Exception e)
      {
   	    String msg = MsgUtils.getMsg("JOBS_SEARCH_LIST_ERROR",threadContext.getJwtTenantId(),threadContext.getJwtUser(), threadContext.getOboTenantId(),threadContext.getOboUser(), e.getMessage());
        _log.error(msg, e);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg,prettyPrint)).build();
      }
      
      if (searchList != null && !searchList.isEmpty()) 
    	  _log.debug("Using searchList. First value = " + searchList.get(0));
     
      // ThreadContext designed to never return null for SearchParameters
      SearchParameters srchParms = threadContext.getSearchParameters();
       
      if(srchParms.getLimit() == null) {srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);}
      
      List<String> selectList = srchParms.getSelectList();
      
      var jobsImpl = JobsImpl.getInstance();
      SelectTuple selectValid = jobsImpl.checkSelectListValidity(selectList);
      _log.debug("select Valid flag : "+ selectValid.getValidFlag() + " str: "+ selectValid.getSelectStr());
      if(!selectValid.getValidFlag()) {
    	  String msg = MsgUtils.getMsg("JOBS_SEARCH_INVALID_SELECTLIST_ERROR",threadContext.getOboTenantId(),threadContext.getOboUser(),selectValid.getSelectStr());
          _log.error(msg);
          return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg,prettyPrint)).build();
      }
      
      boolean allAttributesInResponse = false;
      boolean summaryAttributesInResponse = false;
      // Check if user requested all attributes of each job in the search result response
      if ((!selectList.isEmpty()) && selectList.contains("allAttributes") ) {
   	   allAttributesInResponse = true;
   	   _log.debug("allAttributesInResponse is set to true");
      } else {
    	  if (selectList.isEmpty() || ((!selectList.isEmpty()) && selectList.contains("summaryAttributes"))) {
    	  summaryAttributesInResponse = true;
    	  }
      } 
      
      //Get the computeTotal, default is false
      computeTotal = srchParms.getComputeTotal();
      
      // Get the listType. Default Type is  MY_JOBS   
      if(!EnumUtils.isValidEnum(JobListType.class, listType)) {
    	  String msg = MsgUtils.getMsg("JOBS_SEARCH_INVALID_LISTTYPE_ERROR",threadContext.getJwtTenantId(),threadContext.getJwtUser(),
    			  threadContext.getOboTenantId(),threadContext.getOboUser());
          _log.error(msg);
          return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg,prettyPrint)).build();
      }
      boolean sharedWithMe = false;
      if(listType.equals(JobListType.SHARED_JOBS.name()) || listType.equals(JobListType.ALL_JOBS.name() )){
   	   sharedWithMe = true;
      }
            
     
      
      // summary attributes
      List<JobListDTO> jobSummaryList = new ArrayList<JobListDTO>();
      List<JobListDTO> jobSharedSummaryList = new ArrayList<JobListDTO>();
      
      // All Attributes
      List<Job> jobs = new ArrayList<Job>(); 
      List<Job> jobsShared = new ArrayList<Job>();
     
      
      // Get UUIDs of all jobs shared with the user
      // Note that if sharedWithMe is false, then sharedJobUuidsList will be empty
      
      List<String> sharedJobUuidsList = new ArrayList<String>();
      try {
		sharedJobUuidsList = JobListUtils.getSharedJobUuids(sharedWithMe,threadContext.getOboUser(), threadContext.getOboTenantId() );
      } catch (TapisImplException e) {
   	   _log.error(e.getMessage(), e);
            return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                    entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
      }
      String sharedCond = null;
      // Add the shared jobs UUIDs to the shared searchlist
      if(!sharedJobUuidsList.isEmpty()) {
	       sharedCond = UUID_ATTR + "." + SEARCH_IN_OPERATOR + "." + String.join(",",sharedJobUuidsList);
	       sharedSearchList.addAll(searchList);
	       sharedSearchList.add(sharedCond);
      }     
      
      // --------------------   Compute Total Count -----------------------------------------------
      // If we need the total count and there was a limit then we need to make a call
      int totalCountOwner = 0;
      int totalCountShared = 0;
     
      if (computeTotal){
    	  //totalCountOwner represents all the jobs that the user is owner, admin or creator of the job.
    	  if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
    		  try {
 	        	 totalCountOwner = JobListUtils.computeTotalCount(threadContext.getOboUser(), 
 					   threadContext.getOboTenantId(), searchList, srchParms.getOrderByList(), !SHARED);
 			 } catch (TapisImplException e) {
 					_log.error(e.getMessage(), e);
 			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
 			                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
 			 }
    	  }
    	  //totalCountShared represents all the jobs that are shared with the user
    	  if(sharedWithMe) {
    		  try {
    	           totalCountShared = JobListUtils.computeTotalCount(threadContext.getOboUser(), 
    	 				   threadContext.getOboTenantId(), sharedSearchList, srchParms.getOrderByList(), SHARED);
    	 		 } catch (TapisImplException e) {
    	 				_log.error(e.getMessage(), e);
    	 		           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
    	 		                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
    	 		 }
    	  } 
    	  
    	  if(listType.equals(JobListType.ALL_JOBS.name())){
    		 totalCount = totalCountOwner +  totalCountShared;
    	  } else if (listType.equals(JobListType.MY_JOBS.name())){
    		  totalCount = totalCountOwner;
    	  } else {
    		  totalCount = totalCountShared;
    	  }
     }
     // ----------- Compute Total Ends -------------
      
      int diffLimit = 0;
      int diffSkip = 0;
      // Case 1. User did not specify allAttributes in the select list and select list is empty
      // Default summary attributes will be returned
      if( summaryAttributesInResponse == true){
    	  
    	  // Get the user's jobs in which the user is the owner (no shared jobs)
    	  if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
		       try {
		           jobSummaryList = jobsImpl.getJobSearchListByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), searchList,
		        		   srchParms.getOrderByList(), srchParms.getLimit(),srchParms.getSkip(), !SHARED);                       
		       }
		       catch (TapisImplException e) {
		           _log.error(e.getMessage(), e);
		           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
		                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
		       }
		       catch (Exception e) {
		           _log.error(e.getMessage(), e);
		           return Response.status(Status.INTERNAL_SERVER_ERROR).
		                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
		       }
		     
    	   }
    	  
		    // compute limit and skip for shared list
	    	  if(!jobSummaryList.isEmpty()) {
	    		  diffLimit = srchParms.getLimit() - jobSummaryList.size();
	    		  diffSkip = 0;
	    	  } else {
	    		  // When jobSummaryList is empty, then either all the records for jobs when user is the owner have been skipped 
	    		  // or the list type is SHARED_JOBS
	    		  diffLimit = srchParms.getLimit();
	    		  try {
	  				diffSkip = JobListUtils.computeSkip(listType,threadContext.getOboUser(), 
	  						   threadContext.getOboTenantId(), searchList, srchParms.getOrderByList(), srchParms.getSkip(), !SHARED );
	  			  } catch (TapisImplException e) {
	  				  _log.error(e.getMessage(), e);
	  			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
	  			                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	  			  }
	      }
    	  //------- Get the jobs shared with the user--------------------
	      // Get the shared jobs
	      // Note the sharedSearchList has shared jobs uuids
	       if(sharedWithMe && !sharedJobUuidsList.isEmpty()) {
	    	 try {
				jobSharedSummaryList = 
						jobsImpl.getJobSearchListByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), sharedSearchList,
						   srchParms.getOrderByList(), diffLimit,diffSkip, SHARED);
			    } catch (TapisImplException e) {
			    	 _log.error(e.getMessage(), e);
			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
			                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
			    } 
		        if(!jobSharedSummaryList.isEmpty()) {
		        	jobSummaryList.addAll(jobSharedSummaryList);
		        }
	       }
	       
	       //---------- Get Job shared with user ends ----------------------------//
	       
	       if(jobSummaryList.isEmpty()) {
              String msg =  MsgUtils.getMsg("JOBS_SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
              RespJobSearch r = new RespJobSearch(jobSummaryList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),-1);
              return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg,prettyPrint,r)).build(); 
           }
      
	       if (computeTotal && srchParms.getLimit() <= 0 && srchParms.getSkip() == 0) totalCount = jobSummaryList.size();
	       
	       
	       RespJobSearch r = new RespJobSearch(jobSummaryList, srchParms.getLimit(), srchParms.getOrderBy(),
	    		   srchParms.getSkip(), srchParms.getStartAfter(),totalCount);
	       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
	               MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build(); 
      
      
      } else {
	   	   // select is provided by the user,
	   	   // select all attributes in the sql query to db
	   	   // then select the attributes that the user provides 
    	  if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
    	   try {
		   	   jobs = jobsImpl.getJobSearchAllAttributesByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), searchList,
		        		   srchParms.getOrderByList(), srchParms.getLimit(),srchParms.getSkip(), !(SHARED));  // This is the list of all non-shared jobs                     
		   } catch (TapisImplException e) {
	           _log.error(e.getMessage(), e);
	           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
	                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	       } catch (Exception e) {
	           _log.error(e.getMessage(), e);
	           return Response.status(Status.INTERNAL_SERVER_ERROR).
	                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	       }
    	  }
    	  
    	  // compute limit and skip for shared list
    	  if(!jobs.isEmpty()) {
    		  diffLimit = srchParms.getLimit() - jobs.size();
    		  diffSkip = 0;
    	  } else {
    		  diffLimit = srchParms.getLimit();
    		  try {
				diffSkip = JobListUtils.computeSkip(listType,threadContext.getOboUser(), 
						   threadContext.getOboTenantId(), searchList, srchParms.getOrderByList(), srchParms.getSkip(), !SHARED );
			  } catch (TapisImplException e) {
				  _log.error(e.getMessage(), e);
			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
			                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
			  }
    	  }
	   	   //----------  Get the jobs shared with the user ---------------------------------------
           if(sharedWithMe && !sharedJobUuidsList.isEmpty()) {
	    	  try {
				jobsShared = jobsImpl.getJobSearchAllAttributesByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), sharedSearchList,
						   srchParms.getOrderByList(), diffLimit,diffSkip, SHARED);
			  } catch (TapisImplException e) {
			    	 _log.error(e.getMessage(), e);
			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
			                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
			    }  
		        if(!jobsShared.isEmpty()) {
		        	jobs.addAll(jobsShared);
		        }
	       }
	   	   
	       if(jobs.isEmpty()) {
	          String msg =  MsgUtils.getMsg("JOBS_SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
	          RespJobSearchAllAttributes r = new RespJobSearchAllAttributes(jobs,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
	          return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg,prettyPrint,r)).build(); 
	       }
      }

      if (computeTotal && srchParms.getLimit() <= 0 && srchParms.getSkip() == 0) totalCount = jobs.size();
      
      // customize the response
      if(!selectList.isEmpty() && summaryAttributesInResponse == false && allAttributesInResponse == false ) {
   	  	  RespJobSearchSelectAttributes r = new RespJobSearchSelectAttributes (jobs, selectList, srchParms.getLimit(),
    			  srchParms.getOrderBy(), srchParms.getSkip(), srchParms.getStartAfter(), totalCount);
          return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                  MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build();
   	   
      }
      // ------------------------- Process Results --------------------------
      // Success.
      RespJobSearchAllAttributes r = new RespJobSearchAllAttributes (jobs, srchParms.getLimit(), srchParms.getOrderBy(), 
    		  srchParms.getSkip(), srchParms.getStartAfter(), totalCount);
      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
              MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build();
}
  
    
    /* ---------------------------------------------------------------------------- */
    /* getJobSearchListByPostSqlStr                                                 */
    /* ---------------------------------------------------------------------------- */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            description = "Retrieve list of jobs for the user based on search conditions in the request body and pagination information from the query paramter on the dedicated search end-point.\n\n"
                          + "The caller must be the job owner, creator or a tenant administrator."
                          + "",
            tags = "jobs",
            security = {@SecurityRequirement(name = "TapisJWT")},
            responses = 
                {
                 @ApiResponse(responseCode = "200", description = "Jobs Search List retrieved.",
                     content = @Content(schema = @Schema(
                   		  implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearchAllAttributes.class))),
                 @ApiResponse(responseCode = "400", description = "Input error.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "401", description = "Not authorized.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "403", description = "Forbidden.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
                 @ApiResponse(responseCode = "404", description = "Jobs not found.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
                 @ApiResponse(responseCode = "500", description = "Server error.",
                     content = @Content(schema = @Schema(
                        implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
    )
    public Response getJobSearchListByPostSqlStr (
   		 		@QueryParam("limit") int limit, 
   		 		@QueryParam("skip") int skip,
   		 		@QueryParam("startAfter") int startAfter,
   		 		@QueryParam("orderBy") String orderBy,
   		 		@QueryParam("computeTotal") boolean computeTotal,
   		 		@QueryParam("select") String select,InputStream payloadStream,
   		 	    @DefaultValue("MY_JOBS") @QueryParam("listType") String listType,
                @DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
                              
    {
      // Trace this request.
      if (_log.isTraceEnabled()) {
        String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getJobSearchListByPostSqlStr", 
                                     "  " + _request.getRequestURL());
        _log.trace(msg);
      }
      
      // ------------------------- Create Context ---------------------------
      // Validate the threadlocal content here so no subsequent code on this request needs to.
      TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
      if (!threadContext.validate()) {
          var msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
          _log.error(msg);
          return Response.status(Status.INTERNAL_SERVER_ERROR).
                  entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      
      // ------------------------- Validate Payload -------------------------
      // Read the payload into a string.
      String rawJson = null;
      try {rawJson = IOUtils.toString(payloadStream, Charset.forName("UTF-8"));}
        catch (Exception e) {
          String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "job search", e.getMessage());
          _log.error(msg, e);
          return Response.status(Status.BAD_REQUEST).
                  entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
        }
      
      // ------------------------- Extract and validate payload -------------------------
      // Read the payload into a string.
     
      String msg;
      
      // Create validator specification and validate the json against the schema
      JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_JOB_SEARCH_REQUEST);
      try { JsonValidator.validate(spec); }
      catch (TapisJSONException e)
      {
        msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
        _log.error(msg, e);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
     
      // Construct final SQL-like search string using the json
      // When put together full string must be a valid SQL-like where clause. This will be validated in the service call.
      // Not all SQL syntax is supported. See SqlParser.jj in tapis-shared-searchlib.
      String sqlSearchStr;
      String sqlSearchStrShared = "";
      try
      {
        sqlSearchStr = SearchUtils.getSearchFromRequestJson(rawJson);
        sqlSearchStrShared = sqlSearchStr;
      }
      catch (JsonSyntaxException e)
      {
        msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT",  "job search", e.getMessage());
        _log.error(msg, e);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
      }
      
     // ThreadContext designed to never return null for SearchParameters
     SearchParameters srchParms = threadContext.getSearchParameters();
     int totalCount = DEFAULT_TOTAL_COUNT;
     
     if(srchParms.getLimit() == null) {srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);}
     
     List<String> selectList = srchParms.getSelectList();
     
     var jobsImpl = JobsImpl.getInstance();
     SelectTuple selectValid = jobsImpl.checkSelectListValidity(selectList);
     _log.debug("select Valid flag : "+ selectValid.getValidFlag() + " str: "+ selectValid.getSelectStr());
     if(!selectValid.getValidFlag()) {
   	      msg = MsgUtils.getMsg("JOBS_SEARCH_INVALID_SELECTLIST_ERROR",threadContext.getOboTenantId(),threadContext.getOboUser(),selectValid.getSelectStr());
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg,prettyPrint)).build();
     }
     
     boolean allAttributesInResponse = false;
     boolean summaryAttributesInResponse = false;
     // Check if user requested all attributes of each job in the search result response
     if ((!selectList.isEmpty()) && selectList.contains("allAttributes") ) {
  	   allAttributesInResponse = true;
  	   _log.debug("allAttributesInResponse is set to true");
     } else {
   	  if (selectList.isEmpty() || ((!selectList.isEmpty()) && selectList.contains("summaryAttributes"))) {
   	  summaryAttributesInResponse = true;
   	  }
     } 
     
     //Get the computeTotal, default is false
     computeTotal = srchParms.getComputeTotal();
     
  
     // Get the listType. Default Type is  MY_JOBS   
     if(!EnumUtils.isValidEnum(JobListType.class, listType)) {
   	  msg = MsgUtils.getMsg("JOBS_SEARCH_INVALID_LISTTYPE_ERROR",threadContext.getJwtTenantId(),threadContext.getJwtUser(),
   			  threadContext.getOboTenantId(),threadContext.getOboUser());
         _log.error(msg);
         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg,prettyPrint)).build();
     }
     boolean sharedWithMe = false;
     if(listType.equals(JobListType.SHARED_JOBS.name()) || listType.equals(JobListType.ALL_JOBS.name() )){
  	   sharedWithMe = true;
     }
     // summary attributes
     List<JobListDTO> jobSummaryList = new ArrayList<JobListDTO>();
     List<JobListDTO> jobSharedSummaryList = new ArrayList<JobListDTO>();
     
     // All Attributes
     List<Job> jobs = new ArrayList<Job>(); 
     List<Job> jobsShared = new ArrayList<Job>();
    
     
     // Get UUIDs of all jobs shared with the user
     // Note that if sharedWithMe is false, then sharedJobUuidsList will be empty
     
     List<String> sharedJobUuidsList = new ArrayList<String>();
     try {
		sharedJobUuidsList = JobListUtils.getSharedJobUuids(sharedWithMe,threadContext.getOboUser(), threadContext.getOboTenantId() );
     } catch (TapisImplException e) {
  	   _log.error(e.getMessage(), e);
           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
     }
     
     // Add the shared jobs UUIDs to the shared searchlist
     if(!sharedJobUuidsList.isEmpty()) {
    	 String uuidList="";
    	 int len = sharedJobUuidsList.size();
    	 int i = 0;
    	 for( String uuid :sharedJobUuidsList) {
    		 uuidList = uuidList + QUOTE + uuid + QUOTE;
    		 i++;
    		 if (i<len) {
    			 uuidList = uuidList + COMMA;
    			 
    		 }
    		 
    	 }
    	 //sqlSearchStr.replace("')",")");
	      sqlSearchStrShared = sqlSearchStr + SPACE + AND_OPERATOR  + SPACE + UUID_ATTR + SPACE + SEARCH_IN_OPERATOR + SPACE + LEFT_PARENTHESIS + uuidList + RIGHT_PARENTHESIS;
          _log.debug("sqlSearchStr= " + sqlSearchStrShared);
     }     
     
     // --------------------   Compute Total Count -----------------------------------------------
     // If we need the total count and there was a limit then we need to make a call
     int totalCountOwner = 0;
     int totalCountShared = 0;
    
     if (computeTotal){
   	  //totalCountOwner represents all the jobs that the user is owner, admin or creator of the job.
	   	  if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
	   		  try {
		        	 totalCountOwner = jobsImpl.getJobsSearchListCountByUsernameUsingSqlSearchStr(threadContext.getOboUser(), threadContext.getOboTenantId(),sqlSearchStr,
							   srchParms.getOrderByList(),!SHARED);
				 } catch (TapisImplException e) {
						_log.error(e.getMessage(), e);
				           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
				                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
				 }
	   	  }
	   	  //totalCountShared represents all the jobs that are shared with the user
	   	  if(sharedWithMe) {
	   		  try {
	   	           totalCountShared = jobsImpl.getJobsSearchListCountByUsernameUsingSqlSearchStr(threadContext.getOboUser(), threadContext.getOboTenantId(),sqlSearchStrShared,
						   srchParms.getOrderByList(),SHARED);
	   	 		 } catch (TapisImplException e) {
	   	 				_log.error(e.getMessage(), e);
	   	 		           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
	   	 		                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	   	 		 }
	   	  } 
	   	  
	   	  if(listType.equals(JobListType.ALL_JOBS.name())){
	   		 totalCount = totalCountOwner +  totalCountShared;
	   	  } else if (listType.equals(JobListType.MY_JOBS.name())){
	   		  totalCount = totalCountOwner;
	   	  } else {
	   		  totalCount = totalCountShared;
	   	  }
    }
    // ----------- Compute Total Ends -------------
     
    
     int diffLimit = 0;
     int diffSkip = 0;
     
     if(summaryAttributesInResponse == true) {
      // Get the user's jobs in which the user is the owner (no shared jobs)
   	  if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
		     
	       try {
	          
	         jobSummaryList = jobsImpl.getJobSearchListByUsernameUsingSqlSearchStr(threadContext.getOboUser(), 
	        		 threadContext.getOboTenantId(), sqlSearchStr, srchParms.getOrderByList(), 
	        		 srchParms.getLimit(),srchParms.getSkip(),!SHARED);                       
	       }
	       catch (TapisImplException e) {
	           _log.error(e.getMessage(), e);
	           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
	                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	       }
	       catch (Exception e) {
	           _log.error(e.getMessage(), e);
	           return Response.status(Status.INTERNAL_SERVER_ERROR).
	                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
	       }
	       
   	  }
	       // compute limit and skip for shared list
    	  if(!jobSummaryList.isEmpty()) {
    		  diffLimit = srchParms.getLimit() - jobSummaryList.size();
    		  diffSkip = 0;
    	  } else {
    		  // When jobSummaryList is empty, then either all the records for jobs when user is the owner have been skipped 
    		  // or the list type is SHARED_JOBS
    		  diffLimit = srchParms.getLimit();
    		  try {
  				diffSkip = JobListUtils.computeSkipSqlStr(listType,threadContext.getOboUser(), 
  						   threadContext.getOboTenantId(), sqlSearchStrShared, srchParms.getOrderByList(), srchParms.getSkip(), !SHARED );
  			  } catch (TapisImplException e) {
  				  _log.error(e.getMessage(), e);
  			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
  			                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
  			  }
    	  } 
    	  
	    	//------- Get the jobs shared with the user--------------------
		      // Get the shared jobs
		      // Note the sharedSearchList has shared jobs uuids
		       if(sharedWithMe && !sharedJobUuidsList.isEmpty()) {
		    	 try {
					jobSharedSummaryList = 
							jobsImpl.getJobSearchListByUsernameUsingSqlSearchStr(threadContext.getOboUser(), threadContext.getOboTenantId(), sqlSearchStrShared,
							   srchParms.getOrderByList(), diffLimit,diffSkip, SHARED);
				    } catch (TapisImplException e) {
				    	 _log.error(e.getMessage(), e);
				           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
				                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
				    } 
			        if(!jobSharedSummaryList.isEmpty()) {
			        	jobSummaryList.addAll(jobSharedSummaryList);
			        }
		       }
		       
		       //---------- Get Job shared with user ends ----------------------------//
		       
		       if(jobSummaryList.isEmpty()) {
	              msg =  MsgUtils.getMsg("JOBS_SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
	              RespJobSearch r = new RespJobSearch(jobSummaryList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),-1);
	              return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg,prettyPrint,r)).build(); 
	           }
	      
		       if (computeTotal && srchParms.getLimit() <= 0 && srchParms.getSkip() == 0) totalCount = jobSummaryList.size();
	    	  
	    	
	          RespJobSearch r = new RespJobSearch(jobSummaryList,srchParms.getLimit(), srchParms.getOrderBy(), srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
	          return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
	               MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build(); 
         
   	  } else {
  	   // select is provided by the user,
  	   // select all attributes in the query to db
  	   // then select the attributes that the user provides 
   		if((listType.equals(JobListType.MY_JOBS.name())) || (listType.equals(JobListType.ALL_JOBS.name()))) {
	  	   try {
			   jobs = jobsImpl.getJobSearchAllAttributesByUsernameUsingSqlSearchStr(threadContext.getOboUser(), 
		        		   threadContext.getOboTenantId(), sqlSearchStr, srchParms.getOrderByList(), 
		        		   srchParms.getLimit(), srchParms.getSkip(),!SHARED);                       
		       }
		       catch (TapisImplException e) {
		           _log.error(e.getMessage(), e);
		           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
		                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
		       }
		       catch (Exception e) {
		           _log.error(e.getMessage(), e);
		           return Response.status(Status.INTERNAL_SERVER_ERROR).
		                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
		       }
   		}
	   	 if(!jobs.isEmpty()) {
   		  diffLimit = srchParms.getLimit() - jobs.size();
   		  diffSkip = 0;
	  	 } else {
	   		  diffLimit = srchParms.getLimit();
	   		  try {
					diffSkip = JobListUtils.computeSkipSqlStr(listType,threadContext.getOboUser(), 
							   threadContext.getOboTenantId(), sqlSearchStrShared, srchParms.getOrderByList(), srchParms.getSkip(), !SHARED );
				  } catch (TapisImplException e) {
					  _log.error(e.getMessage(), e);
				           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
				                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
				  }
   	      }
	  	 //----------  Get the jobs shared with the user ---------------------------------------
         if(sharedWithMe && !sharedJobUuidsList.isEmpty()) {
	    	  try {
				jobsShared = jobsImpl.getJobSearchAllAttributesByUsernameUsingSqlSearchStr(threadContext.getOboUser(), threadContext.getOboTenantId(), sqlSearchStrShared,
						   srchParms.getOrderByList(), diffLimit,diffSkip, SHARED);
			  } catch (TapisImplException e) {
			    	 _log.error(e.getMessage(), e);
			           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
			                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
			    }  
		        if(!jobsShared.isEmpty()) {
		        	jobs.addAll(jobsShared);
		        }
	       }
	  	   
	     if(jobs.isEmpty()) {
             msg =  MsgUtils.getMsg("JOBS_SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
             RespJobSearchAllAttributes r = new RespJobSearchAllAttributes(jobs,srchParms.getLimit(),
            		 srchParms.getOrderBy(), srchParms.getSkip(), srchParms.getStartAfter(), totalCount);
             return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg,prettyPrint,r)).build(); 
	     }
       
   	  }

   	  if (computeTotal && srchParms.getLimit() <= 0 && srchParms.getSkip() == 0) totalCount = jobs.size();
     
     // customize the response
     if(!selectList.isEmpty() && summaryAttributesInResponse == false && allAttributesInResponse == false ) {
  	  	  RespJobSearchSelectAttributes r = new RespJobSearchSelectAttributes (jobs, selectList, srchParms.getLimit(),
   			  srchParms.getOrderBy(), srchParms.getSkip(), srchParms.getStartAfter(), totalCount);
         return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
                 MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build();
  	   
     }
     // ------------------------- Process Results --------------------------
     // Success.
     RespJobSearchAllAttributes r = new RespJobSearchAllAttributes (jobs,srchParms.getLimit(),
    		 srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
     
     return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
             MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), 
            		 threadContext.getOboTenantId()), prettyPrint, r)).build();

      
      }
    }
 


