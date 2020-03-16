package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWTParms;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.api.Factory;

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
    try {
      // TODO: remove hard coded values
      // TODO/TBD: Get master tenant from tenant service or from env?
      // Get service master tenant from the env
      String svcMasterTenant = RuntimeParameters.getInstance().getSetServiceMasterTenant();
      // TODO remove hard coded fallback?
      if (StringUtils.isBlank(svcMasterTenant)) svcMasterTenant = "master";
      var svcJWTParms = new ServiceJWTParms();
      svcJWTParms.setTenant(svcMasterTenant);
      // Use TenantManager to get tenant info. Needed for tokens base URLs.
      Tenant tenant = TenantManager.getInstance().getTenant(svcMasterTenant);
      svcJWTParms.setServiceName(SERVICE_NAME_SYSTEMS);
//    svcJWTParms.setTokensBaseUrl("https://dev.develop.tapis.io");
      String tokenSvcUrl = tenant.getTokenService();
      // TODO remove the strip-off once this is cleaned up
      // Strip off everything starting with /v3
      tokenSvcUrl = tokenSvcUrl.substring(0, tokenSvcUrl.indexOf("/v3"));
      svcJWTParms.setTokensBaseUrl(tokenSvcUrl);
      // Get service password from the env
      // TODO remove hard coded fallback
      String svcPassword = RuntimeParameters.getInstance().getServicePassword();
      if (StringUtils.isBlank(svcPassword)) svcPassword = "3qLT0gy3MQrQKIiljEIRa2ieMEBIYMUyPSdYeNjIgZs=";
      return new ServiceJWT(svcJWTParms, svcPassword);
    }
    catch (TapisException e)
    {
      // TODO/TBD Throw exception?
      return null;
    }
  }
  @Override
  public void dispose(ServiceJWT j) {}
}
