package edu.utexas.tacc.tapis.meta.api.jaxrs.filters;

import edu.utexas.tacc.tapis.meta.config.RuntimeParameters;
import edu.utexas.tacc.tapis.meta.utils.MetaAppConstants;
import edu.utexas.tacc.tapis.meta.utils.MetaSKPermissionsMapper;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
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
    // List of todo(s)
    // 1. turn request into a permission spec.
    // 2. utilize the SK client sdk to ask isPermitted
    // 3. decide yes or no based on response
    // 4. add a permission switch for allowAll for testing
    
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
  
    // Tracing.
    if (_log.isTraceEnabled())
      _log.trace("Executing Permissions request filter: " + this.getClass().getSimpleName() + ".");
  
    // let's turn off permissions for a while so we can test without SK client calls
    if(!MetaAppConstants.TAPIS_ENVONLY_META_PERMISSIONS_CHECK){
      _log.trace("Permissions Check is turned OFF!!! " + this.getClass().getSimpleName() + ".");
      return;
    }
    
    // Is this a User request or a Service request?
/*
    if (isUser()) {
    } else {
    }
*/
    
    
    // get a SK client
    //    needs setup of master service token from env
    //    get the path and jwt from runtime parameters
    //
    RuntimeParameters runTime = RuntimeParameters.getInstance();
    
    // Use Meta master token for call to SK
    SKClient skClient = new SKClient(runTime.getSkSvcURL(), runTime.getMetaToken());
    // Service to Service calls require user header, set it to be the same as the service name
    // todo this will change based on header processing so just hardwire for now
    // String TAPIS_USER_HEADER = "X-Tapis-User";
    // String TAPIS_TENANT_HEADER = "X-Tapis-Tenant";
    
    // skClient.addDefaultHeader(MetaAppConstants.TAPIS_USER_HEADER_NAME, "streamsTACCAdmin");
    // skClient.addDefaultHeader(MetaAppConstants.TAPIS_TENANT_HEADER_NAME, "dev");
    
    // 1. If a service receives a request that contains a service JWT,
    //    the request is rejected if it does not have the X-Tapis-Tenant and X-Tapis-User headers set.
    
    // 2. If a service receives a request that has the X-Tapis-Tenant and X-Tapis-User headers set,
    //    the service should forward those header values on  to any service to service call it may make.
    
    // 3. Services should reject any request that contains a user JWT and has the X-Tapis-Tenant
    //    or X-Tapis-User headers set.
    
    // 4. If a service receives a request that contains a user JWT, the service should use the JWT's
    //    tenant and user values as the X-Tapis-Tenant and X-Tapis-User headers on any call it may make
    //    to another service to satisfy the request.
    
    // map the request to permissions
    //      reuse the uri permissions mapper from current rh meta security
    // get the uri from the request, the tenant from the Header the jwt?
    String permissionsSpec = mapRequestToPermissions(requestContext);
    
    // check skClient.isPermitted against the requested uri path
    try {
      skClient.isPermitted("dev","streamsTACCAdmin", permissionsSpec);
    } catch (TapisClientException e) {
      e.printStackTrace();
    }
    
    
  }
  
  private String mapRequestToPermissions(ContainerRequestContext requestContext) {
    String requestMethod = requestContext.getMethod();
    String requestUri = requestContext.getUriInfo().getPath();
    // getting the tenant info
    MetaSKPermissionsMapper mapper = new MetaSKPermissionsMapper(requestUri, "dev");
    String permSpec = mapper.convert(requestMethod);
    
    return permSpec;
  }
  
}
