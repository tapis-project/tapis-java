package edu.utexas.tacc.tapis.security.api.jaxrs.filters;

import java.io.IOException;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.secrets.VaultManager;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This jax-rs filter performs the following:
 * 
 * 
 * This filter is expected to run after all other authentication filters.
 * 
 * @author rcardone
 */
@Provider
@Priority(TapisConstants.JAXRS_FILTER_PRIORITY_AFTER_AUTHENTICATION_2)
public final class SecurityTokenRequestFilter 
implements ContainerRequestFilter
{
   /* ********************************************************************** */
   /*                               Constants                                */
   /* ********************************************************************** */
   // Tracing.
   private static final Logger _log = LoggerFactory.getLogger(SecurityTokenRequestFilter.class);
   
   // The secrets api path prefix.
   private static final String SECRETS_PATH_PREFIX = "/v3/security/secret";

   /* ********************************************************************** */
   /*                                Fields                                  */
   /* ********************************************************************** */
   @Context
   private ResourceInfo resourceInfo;
   
   /* ********************************************************************** */
   /*                            Public Methods                              */
   /* ********************************************************************** */
   /* ---------------------------------------------------------------------- */
   /* filter:                                                                */
   /* ---------------------------------------------------------------------- */
   @Override
   public void filter(ContainerRequestContext requestContext) throws IOException 
   {
       // Tracing.
       if (_log.isTraceEnabled())
           _log.trace("Executing JAX-RX request filter: " + this.getClass().getSimpleName() + ".");
       
       // Get the service-specific path, which is the path after the host:port 
       // segment and includes a leading slash.  
       String relativePath = requestContext.getUriInfo().getRequestUri().getPath();
       
       // We only care about secrets calls.
       if (!relativePath.startsWith(SECRETS_PATH_PREFIX)) return;
       
       // Do we have Vault access.
       var secretsMgr = VaultManager.getInstance(true);
       if (secretsMgr == null || secretsMgr.getSkToken() == null) {
           String msg = MsgUtils.getMsg("SK_VAULT_NOT_AVAILABLE", requestContext.getMethod());
           _log.error(msg);
           requestContext.abortWith(Response.status(Status.SERVICE_UNAVAILABLE).entity(msg).build());
       }
   }
}
