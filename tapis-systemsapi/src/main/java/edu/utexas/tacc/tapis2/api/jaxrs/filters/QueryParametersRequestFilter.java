package edu.utexas.tacc.tapis2.api.jaxrs.filters;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 *  jax-rs filter to intercept various query parameters and set values in the thread context.
 *  Parameters:
 *    pretty - Boolean indicating if response should be pretty printed
 *    TODO limit - Integer indicating maximum number of results to be included.
 *    TODO sort_by, e.g. sort_by=owner(asc),created(desc)
 *    TODO start_after, e.g. systems?limit=10&sort_by=id(asc)&start_after=101
 *
 *  NOTE: Process "pretty" here because it is a parameter for all endpoints and
 *        is not needed for the java client or the back-end (tapis-systemslib)
 *  NOTE: Returning selected attributes in returned results is provided by Jersey's
 *        dynamic filtering (SelectableEntityFilteringFeature). See SystemsApplication.java
 *        So no need to process here. Downside is it is therefore not available in
 *        the java client or calls by some some other front-end to the back-end.
 *  NOTE: "search" is not processed here because we do want it available in the java client.
 *        It is a query parameter in SystemResource.java
 */
@Provider
@Priority(TapisConstants.JAXRS_FILTER_PRIORITY_AFTER_AUTHENTICATION)
public class QueryParametersRequestFilter implements ContainerRequestFilter
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(QueryParametersRequestFilter.class);

  // Query parameter names
  private static final String PARM_PRETTY = "pretty";
//  private static final String PARM_SEARCH = "search";
//  private static final String PARM_LIMIT = "limit";
//  private static final String PARM_AFTER = "start_after";

  /* ********************************************************************** */
  /*                            Public Methods                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* filter:                                                                */
  /* ---------------------------------------------------------------------- */
  @Override
  public void filter(ContainerRequestContext requestContext)
  {
    // Tracing.
    if (_log.isTraceEnabled())
      _log.trace("Executing JAX-RX request filter: " + this.getClass().getSimpleName() + ".");
    // Retrieve all query parameters. If none we are done.
    MultivaluedMap<String, String> queryParameters = requestContext.getUriInfo().getQueryParameters();
    if (queryParameters == null || queryParameters.isEmpty()) return;
    // Get thread context
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();

    // Look for and extract pretty print query parameter.
    if (queryParameters.containsKey(PARM_PRETTY))
    {
      boolean prettyPrint = false;
      // Check that it is a single value and is boolean
      // TODO/TBD: Log error and abort or log warning and continue? Depends on parameter?
      if (queryParameters.get(PARM_PRETTY).size() != 1)
      {
        String msg = "Invalid pretty print query parameter: Multiple values specified.";
        _log.warn(msg);
//        requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(msg).build());
        return;
      }
      String parmValue = queryParameters.get(PARM_PRETTY).get(0);
      // Check that it is a boolean
      _log.error("Found query parameter. Name: " + PARM_PRETTY + " Value: " + parmValue);
      if (!"true".equalsIgnoreCase(parmValue) && !"false".equalsIgnoreCase(parmValue))
      {
        String msg = "Invalid pretty pint query parameter: Must be boolean. Value: " + parmValue;
        _log.warn(msg);
//        requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(msg).build());
        return;
      }
      // Provided parameter is valid. Set as boolean
      prettyPrint = Boolean.parseBoolean(parmValue);
      threadContext.setPrettyPrint(prettyPrint);
    }

//    // Look for and extract search query parameter.
//    if (queryParameters.containsKey(PARM_SEARCH))
//    {
//      _log.trace("Found query parameter. Name: " + PARM_SEARCH);
//      List<String> parmList = queryParameters.get(PARM_SEARCH);
//      if (parmList.size() != 1)
//      {
//        String msg = "Invalid search query parameter: One and only one value must be specified.";
//        _log.error(msg);
//        requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(msg).build());
//        return;
//      }
//      String searchListStr = parmList.get(0);
//      _log.trace("    SearchCondition: " + searchListStr);
//      // TODO Validate search string has form <cond>+<cond>+ ...
//      //      where <cond> = <attr>.<op>.<value>
//      // Parse search string into a list of conditions
//      List<String> searchConditions = Arrays.asList(searchListStr.split("~"));
//      // TODO Validate each condition has the form <attr>.<op>.<value>
//
//      // Add list of search conditions to thread context
//      threadContext.setSearchList(searchConditions);
//    }

// TODO
//    // Look for and extract limit query parameter.
//    if (queryParameters.containsKey(PARM_LIMIT))
//    {
//      int limit = -1;
//      // Check that it is a single value and is an integer
//      // TODO/TBD: Log error and abort or log warning and continue? Depends on parameter?
//      if (queryParameters.get(PARM_LIMIT).size() != 1)
//      {
//        String msg = "Invalid limit query parameter: Multiple values specified.";
//        _log.error(msg);
//        requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(msg).build());
//        return;
//      }
//      String parmValue = queryParameters.get(PARM_LIMIT).get(0);
//      _log.error("Found query parameter. Name: " + PARM_LIMIT + " Value: " + parmValue);
//      // Check that it is an integer
//      try { limit = Integer.parseInt(parmValue); }
//      catch (NumberFormatException e)
//      {
//        String msg = "Invalid limit query parameter: Must be an integer. Value: " + parmValue;
//        _log.error(msg);
//        requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(msg).build());
//        return;
//      }
//      threadContext.setLimit(limit);
//    }

// TODO
//    // Look for and extract start_after query parameter.
//    if (queryParameters.containsKey(PARM_AFTER))
//    {
//      String parmValue = queryParameters.get(PARM_AFTER).get(0);
//      _log.trace("Found query parameter. Name: " + PARM_AFTER + " Value: " + parmValue);
//      if (!StringUtils.isBlank(parmValue)) threadContext.setStartAfter(parmValue);
//    }
  }
}
