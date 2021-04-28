package edu.utexas.tacc.tapis.jobs.dao;


import java.util.Arrays;
import java.util.TreeSet;
import org.jooq.Converter;
@SuppressWarnings({ "serial", "rawtypes" })
public class TagsConverter implements Converter<String[], TreeSet>
{
  public TreeSet<String> from(String[] sa)
  {
    return new TreeSet<>(Arrays.asList(sa));
  }
  public String[] to(TreeSet ts)
  {
    return (String[]) ts.toArray();
  }
  public Class<String[]> fromType() { return String[].class; }
  public Class<TreeSet> toType() { return TreeSet.class; }
}