package edu.utexas.tacc.tapis.meta.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestPropertiesSingleton
{
  // public instance
  public static TestPropertiesSingleton instance;
  private Map<String,String> env;
  
  private TestPropertiesSingleton()
  {
    // create test setups here
    env = new HashMap();
    Properties props = new Properties();
    props.put("baseUrl", "https://dev.develop.tapis.io/");
    
  }
  
  public static TestPropertiesSingleton getInstance(){
    return instance;
  }
  
}

