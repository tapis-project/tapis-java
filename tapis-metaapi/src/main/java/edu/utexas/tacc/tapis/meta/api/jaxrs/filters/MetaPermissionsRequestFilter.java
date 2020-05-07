package edu.utexas.tacc.tapis.meta.api.jaxrs.filters;

import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.meta.utils.MetaAppConstants;
import edu.utexas.tacc.tapis.meta.utils.MetaSKPermissionsMapper;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.security.TapisSecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

import static edu.utexas.tacc.tapis.shared.TapisConstants.SERVICE_NAME_SYSTEMS;

@Provider
@Priority(MetaAppConstants.META_FILTER_PRIORITY_PERMISSIONS)
public class MetaPermissionsRequestFilter implements ContainerRequestFilter {
  
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(MetaPermissionsRequestFilter.class);
  
  
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    // done 1. turn request into a permission spec.
    // done 2. utilize the SK client sdk to ask isPermitted
    // done 3. decide yes or no based on response
    // done 4. add a permission switch for allowAll for testing
    
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    
    // Tracing.
    if (_log.isTraceEnabled())
      _log.trace("Executing Permissions request filter: " + this.getClass().getSimpleName() + ".");
    
    // let's turn off permissions cd for testing without SK client calls
    if(!MetaAppConstants.TAPIS_ENVONLY_META_PERMISSIONS_CHECK){
      _log.trace("Permissions Check is turned OFF!!! " + this.getClass().getSimpleName() + ".");
      return;
    }
    
    //   get the path and jwt from runtime parameters
    RuntimeParameters runTime = RuntimeParameters.getInstance();
    
    //   Use Meta master token for call to SK
    SKClient skClient = new SKClient(runTime.getSkSvcURL(), runTime.getSeviceToken());
    
    //   map the request to permissions
    String permissionsSpec = mapRequestToPermissions(requestContext,threadContext.getJwtTenantId());
    
    // is this request permitted
    boolean isPermitted = false;
    
    // Is this a request with a Service token?
    if(threadContext.getAccountType() == TapisThreadContext.AccountType.service){
      isPermitted = serviceJWT(threadContext, skClient, permissionsSpec);
    }
    
    // Is this a request with a User token?
    if(threadContext.getAccountType() == TapisThreadContext.AccountType.user){
      isPermitted = userJWT(threadContext, skClient, permissionsSpec);
    }
    
    if(!isPermitted){
      String uri = requestContext.getUriInfo().getPath();
          uri = (uri.equals("")) ? "root" : uri;
      StringBuilder msg = new StringBuilder()
          .append("request for this uri path "+uri+" permissions spec ")
          .append(permissionsSpec)
          .append(" is NOT permitted.");
      
      _log.debug(msg.toString());
      requestContext.abortWith(Response.status(Response.Status.FORBIDDEN).entity(msg.toString()).build());
      return;
    }
    
    //------------------------------  permitted to continue  ------------------------------
    StringBuilder msg = new StringBuilder()
        .append("request for this uri permission spec ")
        .append(permissionsSpec)
        .append(" is permitted.");
    
    _log.debug(msg.toString());
  }
  
  /**
   * Check Permissions based on service JWT from request.
   * make an isPermitted request of the SK
   * @param threadContext
   * @param skClient
   * @param permissionsSpec
   */
  private boolean serviceJWT(TapisThreadContext threadContext, SKClient skClient, String permissionsSpec){
    // 1. If a service receives a request that contains a service JWT,
    //    the request is rejected if it does not have the X-Tapis-Tenant and X-Tapis-User headers set.
    // **** this check happens in the JWT filter
    
    // 2. If a service receives a request that has the X-Tapis-Tenant and X-Tapis-User headers set,
    //    the service should forward those header values on  to any service to service call it may make.
    // skClient was created with metav3 master service token.
    
    boolean isPermitted = false;
    
    skClient.addDefaultHeader(MetaAppConstants.TAPIS_USER_HEADER_NAME, threadContext.getOboUser());
    skClient.addDefaultHeader(MetaAppConstants.TAPIS_TENANT_HEADER_NAME, threadContext.getOboTenantId());
    
    // check skClient.isPermitted against the requested uri path
    try {
      // checking obo tenant and user
      StringBuilder msg = new StringBuilder();
      msg.append("Permissions check for Tenant ")
         .append(threadContext.getOboTenantId())
         .append(", User ")
         .append(threadContext.getOboUser())
         .append(" with permissions ")
         .append(permissionsSpec+".");
      
      _log.debug(msg.toString());
      isPermitted = skClient.isPermitted(threadContext.getOboTenantId(),threadContext.getOboUser(), permissionsSpec);
    } catch (TapisClientException e) {
      // todo add log msg for exception
      e.printStackTrace();
    }
    
    return isPermitted;
  }
  
  private boolean userJWT(TapisThreadContext threadContext, SKClient skClient, String permissionsSpec){
    // 3. Services should reject any request that contains a user JWT and has the X-Tapis-Tenant
    //    or X-Tapis-User headers set.
    // assume this is already done via the jwt filter
    
    // 4. If a service receives a request that contains a user JWT, the service should use the JWT's
    //    tenant and user values as the X-Tapis-Tenant and X-Tapis-User headers on any call it may make
    //    to another service to satisfy the request.
    
    boolean isPermitted = false;
    
    // set X-Tapis-Tenant and X-Tapis-User headers with jwtTenant & jwtUser
    // with meta service master token
    skClient.addDefaultHeader(MetaAppConstants.TAPIS_USER_HEADER_NAME, threadContext.getJwtUser());
    skClient.addDefaultHeader(MetaAppConstants.TAPIS_TENANT_HEADER_NAME, threadContext.getJwtTenantId());
    
    // check skClient.isPermitted against the requested uri path
    try {
      // checking obo tenant and user
      isPermitted = skClient.isPermitted(threadContext.getOboTenantId(),threadContext.getOboUser(), permissionsSpec);
    } catch (TapisClientException e) {
      // todo add log msg for exception
      e.printStackTrace();
    }
    
    return isPermitted;
  }
  
  
  /**
   * Turn the request uri into a SK permissions spec to check authorization
   * @param requestContext
   * @return  the String representing a permissions spec for comparison
   */
  private String mapRequestToPermissions(ContainerRequestContext requestContext, String tenantId) {
    String requestMethod = requestContext.getMethod();
    String requestUri = requestContext.getUriInfo().getPath();
    // getting the tenant info
    // TODO pull tenant info for checking permissions
    MetaSKPermissionsMapper mapper = new MetaSKPermissionsMapper(requestUri, tenantId);
    String permSpec = mapper.convert(requestMethod);
    
    return permSpec;
  }
  
}
