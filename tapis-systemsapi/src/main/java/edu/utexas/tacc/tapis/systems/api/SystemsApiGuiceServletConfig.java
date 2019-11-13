package edu.utexas.tacc.tapis.systems.api;

//import com.google.inject.AbstractModule;
//import com.google.inject.Guice;
//import com.google.inject.Injector;
//import com.google.inject.Module;
//import com.google.inject.servlet.GuiceServletContextListener;
//import com.google.inject.servlet.ServletModule;
//import edu.utexas.tacc.tapis.systems.SystemsLibModule;
////import io.logz.guice.jersey.JerseyModule;
////import io.logz.guice.jersey.configuration.JerseyConfiguration;
//
//import java.util.ArrayList;
//import java.util.List;

//import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;

//import com.google.inject.servlet.ServletModule;
//import com.squarespace.jersey2.guice.JerseyGuiceServletContextListener;

//import java.util.Collections;
//import java.util.List;

//public class SystemsApiGuiceServletConfig extends JerseyGuiceServletContextListener
//{
//  @Override
//  protected List<? extends Module> modules() {
//    return Collections.singletonList(
//      new ServletModule() {
//      @Override
//      protected void configureServlets() {
////        bind(SystemsDao.class).to(SystemsDaoImpl.class);
//      }
//    }
//    );
//  }
//}
public class SystemsApiGuiceServletConfig //extends GuiceServletContextListener
{
//  @Override
//  protected Injector getInjector() {
////    JerseyConfiguration configuration = JerseyConfiguration.builder()
////      .addPackage("edu.utexas.tacc.tapis.systems.api")
////      .addPackage("edu.utexas.tacc.tapis.systems.api.resources")
////      .addPort(8080)
////      .build();
////
////    List<Module> modules = new ArrayList<>();
////    modules.add(new JerseyModule(configuration));
////    modules.add(new SystemsLibModule());
//////    modules.add(new AbstractModule() {
//////      @Override
//////      protected void configure() {
//////        // Your module bindings ...
//////      }
//////    });
////    return Guice.createInjector(modules);
//
//
////    return Guice.createInjector(new ServletModule());
//
//    return Guice.createInjector(new SystemsLibModule(), new ServletModule());
//  }
}
