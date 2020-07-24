package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWTParms;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.api.Factory;

import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.SYSTEMS_DEFAULT_MASTER_TENANT;
import static edu.utexas.tacc.tapis.shared.TapisConstants.SERVICE_NAME_SYSTEMS;

/**
 * HK2 Factory class providing a ServiceJWT for the systems service.
 * Binding happens in SystemsApplication.java
 */
public class SystemsServiceJWTFactory implements Factory<ServiceJWT>
{
  @Override
  public ServiceJWT provide()
  {
    String svcMasterTenant = null;
    String tokenSvcUrl = null;
    try {
      // TODO: remove hard coded values
      // TODO/TBD: Get master tenant from tenant service or from env? Keep hard coded default?
      // Get service master tenant from the env
      svcMasterTenant = RuntimeParameters.getInstance().getServiceMasterTenant();
      if (StringUtils.isBlank(svcMasterTenant)) svcMasterTenant = SYSTEMS_DEFAULT_MASTER_TENANT;
      var svcJWTParms = new ServiceJWTParms();
      svcJWTParms.setTenant(svcMasterTenant);
      // Use TenantManager to get tenant info. Needed for tokens base URLs. E.g. https://dev.develop.tapis.io
      Tenant tenant = TenantManager.getInstance().getTenant(svcMasterTenant);
      svcJWTParms.setServiceName(SERVICE_NAME_SYSTEMS);
      tokenSvcUrl = tenant.getTokenService();
      // TODO remove the strip-off once no longer needed
      // Strip off everything starting with /v3
      tokenSvcUrl = tokenSvcUrl.substring(0, tokenSvcUrl.indexOf("/v3"));
      svcJWTParms.setTokensBaseUrl(tokenSvcUrl);
      // Get service password from the env
      String svcPassword = RuntimeParameters.getInstance().getServicePassword();
      if (StringUtils.isBlank(svcPassword))
      {
        String msg = LibUtils.getMsg("SYSLIB_NO_SVC_PASSWD", svcMasterTenant, tokenSvcUrl);
        throw new RuntimeException(msg);
      }
      return new ServiceJWT(svcJWTParms, svcPassword);
    }
    catch (TapisException | TapisClientException te)
    {
      String msg = LibUtils.getMsg("SYSLIB_SVCJWT_ERROR", svcMasterTenant, tokenSvcUrl);
      throw new RuntimeException(msg, te);
    }
  }
  @Override
  public void dispose(ServiceJWT j) {}
}
