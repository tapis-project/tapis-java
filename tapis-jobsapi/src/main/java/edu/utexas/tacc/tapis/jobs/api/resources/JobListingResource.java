package edu.utexas.tacc.tapis.jobs.api.resources;


import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.api.responses.RespGetJobList;
import edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearch;
import edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearchAllAttributes;
import edu.utexas.tacc.tapis.jobs.api.utils.JobsApiUtils;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.dto.JobListDTO;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

	@Path("/")
	public class JobListingResource 
	 extends AbstractResource
	{
	    /* **************************************************************************** */
	    /*                                   Constants                                  */
	    /* **************************************************************************** */
	    // Local logger.
	    private static final Logger _log = LoggerFactory.getLogger(JobListingResource.class);
	    
	    
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
	     /* getJobList:                                                                      */
	     /* ---------------------------------------------------------------------------- */
	     @GET
	     @Produces(MediaType.APPLICATION_JSON)
	     @Operation(
	             description = "Retrieve list of jobs for the user.\n\n"
	                           + "The caller must be the job owner, creator or a tenant administrator."
	                           + "",
	             tags = "jobs",
	             security = {@SecurityRequirement(name = "TapisJWT")},
	             responses = 
	                 {
	                  @ApiResponse(responseCode = "200", description = "Jobs List retrieved.",
	                      content = @Content(schema = @Schema(
	                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespGetJobList.class))),
	                  @ApiResponse(responseCode = "400", description = "Input error.",
	                      content = @Content(schema = @Schema(
	                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
	                  @ApiResponse(responseCode = "401", description = "Not authorized.",
	                      content = @Content(schema = @Schema(
	                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
	                  @ApiResponse(responseCode = "403", description = "Forbidden.",
	                      content = @Content(schema = @Schema(
	                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class))),
	                  @ApiResponse(responseCode = "404", description = "Job not found.",
	                      content = @Content(schema = @Schema(
	                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespName.class))),
	                  @ApiResponse(responseCode = "500", description = "Server error.",
	                      content = @Content(schema = @Schema(
	                         implementation = edu.utexas.tacc.tapis.sharedapi.responses.RespBasic.class)))}
	     )
	     public Response getJobList(
	    		 				@QueryParam("limit") int limit, 
	    		 				@QueryParam("skip") int skip,
	    		 				@QueryParam("startAfter") int startAfter,
	    		 				@QueryParam("orderBy") String OrderBy,
	    		 				@QueryParam("computeTotal")  boolean computeTotal,
	    		 				@DefaultValue("false") @QueryParam("pretty") boolean prettyPrint)
	                               
	     {
	       // Trace this request.
	       if (_log.isTraceEnabled()) {
	         String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), "getJobList", 
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
	       
	       // ------ Set default values for the reserved query parameters ------------
	       // orderBy is of the format - fname1(DESC), fname2, fname3(ASC), ...
	       // ThreadContext designed to never return null for SearchParameters
	       SearchParameters srchParms = threadContext.getSearchParameters();
	        
	       if(srchParms.getLimit() == null) {srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);}
	       int totalCount = -1; 
	       computeTotal = srchParms.getComputeTotal(); 
	       // ------------------------- Retrieve Job List -----------------------------
	       List<JobListDTO> jobList = null;
	       var jobsImpl = JobsImpl.getInstance();
	          
	       try {
	           
	           jobList = jobsImpl.getJobListByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(),
	        		   srchParms.getOrderByList(), srchParms.getLimit(),srchParms.getSkip());                       
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
	       if(jobList.isEmpty()) {
               String msg =  MsgUtils.getMsg("SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
               RespGetJobList r = new RespGetJobList(jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
               return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse( msg,prettyPrint,r)).build(); 
           }
	        // -------------------- Calculate the total count --------------------
	      	       
	       // If we need the count and there was a limit then we need to make a call
	       List<String>searchList = srchParms.getSearchList();
	       if (computeTotal && srchParms.getLimit() > 0)
	       {
	         
					try {
						totalCount = jobsImpl.getJobsSearchListCountByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), searchList,
								   srchParms.getOrderByList());
					} catch (TapisImplException e) {
						_log.error(e.getMessage(), e);
				           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
				                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
					}
				
		    } else if (computeTotal && srchParms.getLimit() <= 0) {
		    	totalCount = jobList.size();
		    } else {
		    	totalCount = -1;
		    	
		    }
		    
	       // ------------------------- Process Results --------------------------
	       // Success.
	      
	       RespGetJobList r = new RespGetJobList(jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
	       //RespJobSearchAllAttributes r = new RespJobSearchAllAttributes (jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
	       return Response.status(Status.OK).entity(TapisRestUtils
	    		   .createSuccessResponse(
	               MsgUtils.getMsg("JOBS_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build();
	     }
	     
	     
	     /* ---------------------------------------------------------------------------- */
	     /* getJobSearchList:                                                                      */
	     /* ---------------------------------------------------------------------------- */
	     @GET
	     @Path("/search")
	     @Produces(MediaType.APPLICATION_JSON)
	     @Operation(
	             description = "Retrieve list of jobs for the user based on search conditions in the query paramter on the dedicsted search end-point.\n\n"
	                           + "The caller must be the job owner, creator or a tenant administrator."
	                           + "",
	             tags = "jobs",
	             security = {@SecurityRequirement(name = "TapisJWT")},
	             responses = 
	                 {
	                  @ApiResponse(responseCode = "200", description = "Jobs Search List retrieved.",
	                      content = @Content(schema = @Schema(
	                         implementation = edu.utexas.tacc.tapis.jobs.api.responses.RespJobSearch.class))),
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
	       List<String> searchList;
	       int totalCount = -1;
	       try
	       {
	         searchList = SearchUtils.buildListFromQueryParms(_uriInfo.getQueryParameters());
	       }
	       catch (Exception e)
	       {
	    	  String msg = MsgUtils.getMsg("SEARCH_LIST_ERROR",threadContext.getJwtTenantId(),threadContext.getJwtUser(), threadContext.getOboTenantId(),threadContext.getOboUser(), e.getMessage());
	         _log.error(msg, e);
	         return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg,prettyPrint)).build();
	       }
	       if (searchList != null && !searchList.isEmpty()) _log.debug("Using searchList. First value = " + searchList.get(0));
	       // ThreadContext designed to never return null for SearchParameters
	       SearchParameters srchParms = threadContext.getSearchParameters();
	        
	       if(srchParms.getLimit() == null) {srchParms.setLimit(SearchParameters.DEFAULT_LIMIT);}
	       List<String> selectList = srchParms.getSelectList();
	       boolean allAttributesInResponse = false;
	       if ((!selectList.isEmpty() ) && selectList.contains("allAttributes") ) {
	    	   allAttributesInResponse = true;
	    	   _log.debug("allAttributesInResponse is set to true");
	       } ;
	       computeTotal = srchParms.getComputeTotal();
	       
	       var jobsImpl = JobsImpl.getInstance();
	       
	       // summary attributes
	       List<JobListDTO> jobList = null;
	       List<Job> jobs = null; 
	       
	       // If we need the count and there was a limit then we need to make a call
	       if (computeTotal && srchParms.getLimit() > 0)
	       {
	         
					try {
						totalCount = jobsImpl.getJobsSearchListCountByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), searchList,
								   srchParms.getOrderByList());
					} catch (TapisImplException e) {
						_log.error(e.getMessage(), e);
				           return Response.status(JobsApiUtils.toHttpStatus(e.condition)).
				                   entity(TapisRestUtils.createErrorResponse(e.getMessage(), prettyPrint)).build();
					}
				
		    }
	       
	       if(allAttributesInResponse == false && selectList.isEmpty() ) {
		       try {
		          
		          
		           jobList = jobsImpl.getJobSearchListByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), searchList,
		        		   srchParms.getOrderByList(), srchParms.getLimit(),srchParms.getSkip());                       
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
		       
		       if(jobList.isEmpty()) {
	               String msg =  MsgUtils.getMsg("JOBS_SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
	               RespJobSearch r = new RespJobSearch(jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),-1);
	               return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg,prettyPrint,r)).build(); 
	            }
	       
	       
		       if (computeTotal && srchParms.getLimit() <= 0) totalCount = jobList.size();
		       RespJobSearch r = new RespJobSearch(jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
		       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
		               MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build(); 
	       
	       } else {
	    	   // select is provided by the user,
	    	   // select all attributes in the query to db
	    	   // then select the attributes that the user provides 
	    	   try {
			          
			          
		           jobs = jobsImpl.getJobSearchAllAttributesByUsername(threadContext.getOboUser(), threadContext.getOboTenantId(), searchList,
		        		   srchParms.getOrderByList(), srchParms.getLimit(),srchParms.getSkip());                       
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
		       
		       if(jobs.isEmpty()) {
	               String msg =  MsgUtils.getMsg("JOBS_SEARCH_NO_JOBS_FOUND", threadContext.getOboTenantId(),threadContext.getOboUser());
	               RespJobSearch r = new RespJobSearch(jobList,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),-1);
	               return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg,prettyPrint,r)).build(); 
	            }
	       }

	       if (computeTotal && srchParms.getLimit() <= 0) totalCount = jobs.size();
	       
	       // customize the response
	       if(!selectList.isEmpty() && !selectList.contains("allAttributes") && !selectList.contains("summaryAttributes") ) {
	    	  //TODO selectable fields
	    	   
	       }
	       // ------------------------- Process Results --------------------------
	       // Success.
	       RespJobSearchAllAttributes r = new RespJobSearchAllAttributes (jobs,srchParms.getLimit(),srchParms.getOrderBy(),srchParms.getSkip(),srchParms.getStartAfter(),totalCount);
	       
	       
	       return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
	               MsgUtils.getMsg("JOBS_SEARCH_RESULT_LIST_RETRIEVED", threadContext.getOboUser(), threadContext.getOboTenantId()), prettyPrint, r)).build();

		  
	     }
	}



