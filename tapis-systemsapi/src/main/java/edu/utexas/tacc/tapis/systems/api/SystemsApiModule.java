package edu.utexas.tacc.tapis.systems.api;

import com.google.inject.AbstractModule;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl;
import edu.utexas.tacc.tapis.systems.service.SystemsService;

/*
 * Guice module for SystemsApi
 */
public class SystemsApiModule extends AbstractModule
{
  @Override
  protected void configure()
  {
    bind(SystemsService.class).to(SystemsServiceImpl.class);
  }
}
