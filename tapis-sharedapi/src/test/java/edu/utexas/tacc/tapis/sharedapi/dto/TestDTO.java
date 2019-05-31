package edu.utexas.tacc.tapis.sharedapi.dto;

import java.time.Instant;
import java.util.HashMap;

public class TestDTO 
{
  private String  string1 = "xxx";
  private int     int1 = 88;
  private Boolean bool1 = true;
  private HashMap<String,Integer> map1 = new HashMap<String,Integer>();
  private Instant[] instanceArray = {Instant.EPOCH, Instant.MAX};
  
  public TestDTO()
  {
    map1.put("Four", 4);
    map1.put("Two", 2);
  }
}
