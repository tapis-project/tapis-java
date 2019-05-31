package edu.utexas.tacc.aloe.sharedapi.jaxrs.filters;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.aloe.shared.AloeConstants;
import edu.utexas.tacc.aloe.shared.threadlocal.AloeThreadLocal;

/** This jax-rs filter clears the aloe thread local value after a request
 * has been processed.
 * 
 * @author rcardone
 */
@Provider
@Priority(AloeConstants.JAXRS_FILTER_PRIORITY_BEFORE_AUTHENTICATION)
public class ClearThreadLocalResponseFilter 
 implements ContainerResponseFilter
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ClearThreadLocalResponseFilter.class);
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* filter:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
    {
        // Tracing.
        if (_log.isTraceEnabled())
            _log.trace("Executing JAX-RX response filter: " + this.getClass().getSimpleName() + ".");
        
        // Remove any existing aloe threadlocal information.
        AloeThreadLocal.aloeThreadContext.remove();
    }
}
