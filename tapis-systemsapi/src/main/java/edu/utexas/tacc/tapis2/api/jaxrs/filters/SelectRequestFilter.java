package edu.utexas.tacc.tapis2.api.jaxrs.filters;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;
import java.util.List;

/**
 *  This jax-rs filter intercepts the query parameter "select" and updates a value in the thread context.
 */
@Provider
@Priority(TapisConstants.JAXRS_FILTER_PRIORITY_AFTER_AUTHENTICATION)
public class SelectRequestFilter implements ContainerRequestFilter
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SelectRequestFilter.class);

  private static final String PARM_NAME = "select";

  /* ********************************************************************** */
  /*                            Public Methods                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* filter:                                                                */
  /* ---------------------------------------------------------------------- */
  @Override
  public void filter(ContainerRequestContext requestContext) {
    // Tracing.
    if (_log.isTraceEnabled())
      _log.trace("Executing JAX-RX request filter: " + this.getClass().getSimpleName() + ".");
    // TODO *****************************************************8
    // TODO POC for intercepting query parameter
    // TODO *****************************************************8
    // Look for query parameter.
    // If not found then we are done.
    // If found then extract value and remove the parameter.
    MultivaluedMap<String, String> queryParameters = requestContext.getUriInfo().getQueryParameters();
    if (queryParameters == null || !queryParameters.containsKey(PARM_NAME)) return;
    // TODO Since we will remove the query param we need to validate it here
    List<String> parmList = queryParameters.get(PARM_NAME);
//    if (parmList.size() > 2) {
//      String msg = "Invalid query parameter: Too many values specified. Query parm: " + PARM_NAME;
//      _log.error(msg);
//      requestContext.abortWith(Response.status(Response.Status.BAD_REQUEST).entity(msg).build());
//      return;
//    }
    String parmValue = queryParameters.get(PARM_NAME).get(0);
    _log.error("Found query parameter. Name: " + PARM_NAME + "First value: " + parmValue);
    // Remove query parameter.
//    UriBuilder uriBuilder = requestContext.getUriInfo().getRequestUriBuilder();
//    uriBuilder.replaceQueryParam(PARM_NAME, (Object[]) null).build();
//    requestContext.setRequestUri(uriBuilder.build());
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
//    threadContext.setSelectList(parmList);
  }
}
