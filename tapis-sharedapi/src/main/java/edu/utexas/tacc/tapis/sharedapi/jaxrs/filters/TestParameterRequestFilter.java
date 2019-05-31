package edu.utexas.tacc.tapis.sharedapi.jaxrs.filters;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;

/** Determine if test parameter usage is enabled.  This filter runs after the
 * JWT authentication filter and can change some values set by that filter.
 * 
 * @author rcardone
 */
@Provider
@Priority(TapisConstants.JAXRS_FILTER_PRIORITY_AFTER_AUTHENTICATION)
public class TestParameterRequestFilter 
 implements ContainerRequestFilter
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(TestParameterRequestFilter.class);
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    
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
        
        // Determine if we are ignoring or respecting test parameters.
        if (!TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_ALLOW_TEST_QUERY_PARMS)) return;
        
        // Initialize all test parameter values.
        String tenantId = null;
        String user = null;
        
        // See if the query parameters contain test values.
        UriInfo uriInfo = requestContext.getUriInfo();
        if (uriInfo == null) return;
        MultivaluedMap<String,String> queryParms = uriInfo.getQueryParameters();
        if (queryParms == null || queryParms.isEmpty()) return;
        tenantId = queryParms.getFirst(TapisConstants.QUERY_PARM_TEST_TENANT);
        user = queryParms.getFirst(TapisConstants.QUERY_PARM_TEST_USER);
        
        // Assign non-null test values to thread-local variables.
        TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
        if (!StringUtils.isBlank(tenantId)) threadContext.setTenantId(tenantId);
        if (!StringUtils.isBlank(user)) threadContext.setUser(user);
    }
}
