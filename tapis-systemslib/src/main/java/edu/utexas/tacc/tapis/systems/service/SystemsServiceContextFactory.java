package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import static edu.utexas.tacc.tapis.shared.TapisConstants.SERVICE_NAME_SYSTEMS;

import org.glassfish.hk2.api.Factory;

/**
 * HK2 Factory class providing a ServiceContext for the systems service.
 * ServiceContext is a singleton used to manage JWTs.
 * Binding happens in SystemsApplication.java
 */
public class SystemsServiceContextFactory implements Factory<ServiceContext>
{
  @Override
  public ServiceContext provide()
  {
    ServiceContext svcContext = ServiceContext.getInstance();

    String svcMasterTenant = null;
    String tokenSvcUrl = null;
    RuntimeParameters runParms = RuntimeParameters.getInstance();
    try {
      svcContext.initServiceJWT(runParms.getSiteId(), SERVICE_NAME_SYSTEMS, runParms.getServicePassword());
// TODO remove
// ???
//      // TODO: remove hard coded values
//      // TODO/TBD: Get master tenant from tenant service or from env? Keep hard coded default?
//      RuntimeParameters runParms = RuntimeParameters.getInstance();
//      // Get service master tenant from the env
//      svcMasterTenant = runParms.getServiceMasterTenant();
//      if (StringUtils.isBlank(svcMasterTenant)) svcMasterTenant = SYSTEMS_DEFAULT_MASTER_TENANT;
//      var svcJWTParms = new ServiceJWTParms();
//      svcJWTParms.setTenant(svcMasterTenant);
//      // TODO: FIX-FOR-ASSOCIATE-SITES
//      // TODO: How to get full list of sites?
//      svcJWTParms.setTargetSites(Arrays.asList(runParms.getSiteId()));
//      // Use TenantManager to get tenant info. Needed for tokens base URLs. E.g. https://dev.develop.tapis.io
//      Tenant tenant = TenantManager.getInstance().getTenant(svcMasterTenant);
//      svcJWTParms.setServiceName(SERVICE_NAME_SYSTEMS);
//      tokenSvcUrl = tenant.getTokenService();
//      // TODO remove the strip-off once no longer needed
//      // Strip off everything starting with /v3
//      tokenSvcUrl = tokenSvcUrl.substring(0, tokenSvcUrl.indexOf("/v3"));
//      svcJWTParms.setTokensBaseUrl(tokenSvcUrl);
//      // Get service password from the env
//      String svcPassword = runParms.getServicePassword();
//      if (StringUtils.isBlank(svcPassword))
//      {
//        String msg = LibUtils.getMsg("SYSLIB_NO_SVC_PASSWD", svcMasterTenant, tokenSvcUrl);
//        throw new RuntimeException(msg);
//      }
//      return new ServiceJWT(svcJWTParms, svcPassword);
// ???
      return svcContext;
    }
    catch (TapisException | TapisClientException te)
    {
      String msg = LibUtils.getMsg("SYSLIB_SVCJWT_ERROR", svcMasterTenant, tokenSvcUrl);
      throw new RuntimeException(msg, te);
    }
  }
  @Override
  public void dispose(ServiceContext c) {}
}
