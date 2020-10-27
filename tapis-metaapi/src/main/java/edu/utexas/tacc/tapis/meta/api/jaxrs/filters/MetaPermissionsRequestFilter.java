package edu.utexas.tacc.tapis.meta.api.jaxrs.filters;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.meta.utils.MetaAppConstants;
import edu.utexas.tacc.tapis.meta.utils.MetaSKPermissionsMapper;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.annotation.security.PermitAll;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Provider
@Priority(MetaAppConstants.META_FILTER_PRIORITY_PERMISSIONS)
public class MetaPermissionsRequestFilter implements ContainerRequestFilter {
  @Context
  private ResourceInfo resourceInfo;
  
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
    if (_log.isTraceEnabled()){
      _log.trace("Executing Permissions request filter: " + this.getClass().getSimpleName() + ".");
      // debugRequestDump(threadContext,requestContext);
    }
    
    // let's turn off permissions for testing without SK client calls
    if(!MetaAppConstants.TAPIS_ENVONLY_META_PERMISSIONS_CHECK){
      _log.trace("Permissions Check is turned OFF!!! " + this.getClass().getSimpleName() + ".");
      return;
    }
  
    // @PermitAll on the method takes precedence over @RolesAllowed on the class, allow all
    // requests with @PermitAll to go through
    if (resourceInfo.getResourceMethod().isAnnotationPresent(PermitAll.class)) return;
  
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
    }else if(threadContext.getAccountType() == TapisThreadContext.AccountType.user){      // Is this a request with a User token?
      isPermitted = userJWT(threadContext, skClient, permissionsSpec);
    }
    
    if(!isPermitted){
      String uri = requestContext.getUriInfo().getPath();
          uri = (uri.equals("")) ? "root" : uri;
      StringBuilder msg = new StringBuilder()
          .append("request for this uri path "+uri+" permissions spec ")
          .append(permissionsSpec)
          .append(" is NOT permitted.");
      
      _log.info(msg.toString());
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
  
  /*------------------------------------------------------------------------
   * serviceJWT
   * -----------------------------------------------------------------------*/
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
    //    skClient was created with metav3 master service token.
  
    boolean isPermitted = false;
  
    // these we being used as of 10.26/2020
    // skClient.addDefaultHeader(MetaAppConstants.TAPIS_USER_HEADER_NAME, threadContext.getOboUser());
    // skClient.addDefaultHeader(MetaAppConstants.TAPIS_TENANT_HEADER_NAME, threadContext.getOboTenantId());
    skClient.addDefaultHeader(MetaAppConstants.TAPIS_USER_HEADER_NAME, threadContext.getJwtUser());
    skClient.addDefaultHeader(MetaAppConstants.TAPIS_TENANT_HEADER_NAME, threadContext.getJwtTenantId());
  
    // check skClient.isPermitted against the requested uri path
    try {
      // checking obo tenant and user
      StringBuilder msg = new StringBuilder();
      msg.append("Permissions check for Tenant ")
         .append(threadContext.getJwtTenantId())
         .append(", User ")
         .append(threadContext.getJwtUser())
         .append(" with permissions ")
         .append(permissionsSpec+".");
    
      _log.debug(msg.toString());
      isPermitted = skClient.isPermitted(threadContext.getJwtTenantId(), threadContext.getJwtUser(), permissionsSpec);
    } catch (TapisClientException e) {
      StringBuilder msg = new StringBuilder();
      msg.append("SKClient threw and exception on call to SK ")
         .append(e.getTapisMessage());
  
      _log.debug(msg.toString());
    }
    
    return isPermitted;
  }
  
  /*------------------------------------------------------------------------
   * userJWT
   * -----------------------------------------------------------------------*/
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
      isPermitted = skClient.isPermitted(threadContext.getJwtTenantId(), threadContext.getJwtUser(), permissionsSpec);
    } catch (TapisClientException e) {
      StringBuilder msg = new StringBuilder();
      msg.append("SKClient threw and exception on call to SK ")
         .append(e.getTapisMessage());
  
      _log.debug(msg.toString());
    }
    
    return isPermitted;
  }
  
  
  /**
   * Turn the request uri into a SK permissions spec to check authorization
   * @param requestContext
   * @return  the String representing a permissions spec for comparison. "" empty String if unsuccessful.
   */
  private String mapRequestToPermissions(ContainerRequestContext requestContext, String tenantId) {
    String requestMethod = requestContext.getMethod();
    String requestUri = requestContext.getUriInfo().getPath();
    // getting the tenant info
    MetaSKPermissionsMapper mapper = null;
    String permSpec = "";
    if(!tenantId.isEmpty()){
      mapper = new MetaSKPermissionsMapper(requestUri, tenantId);
    }else{
      return permSpec;
    }
    permSpec = mapper.convert(requestMethod);
    return permSpec;
  }
  
  private void debugRequestDump(TapisThreadContext threadContext,ContainerRequestContext requestContext){
    _log.trace("Thread context ");
    _log.trace("  account : "+threadContext.getAccountType().toString());
    _log.trace("  tenant  : "+threadContext.getJwtTenantId());
    _log.trace("  user    : "+threadContext.getJwtUser());
    _log.trace("Request context ");
    _log.trace("Headers  ");
    MultivaluedMap<String,String> headers = requestContext.getHeaders();
    headers.forEach((k, v)->{ _log.trace("      Key: " + k + " Value: " + v); });

  }
  
  
}
