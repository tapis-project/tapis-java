package edu.utexas.tacc.tapis.systems.dao;

import org.jooq.Converter;
import org.jooq.EnumType;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

public class TransferMethodArrayConverter implements Converter<String[], TransferMethod[]>
{
  public TransferMethod[] from(String[] sa)
  {
    TransferMethod[] ta = {};
    return ta;
  }

  public String[] to(TransferMethod[] ta)
  {
    String[] sa = {};
    return sa;
  }

  public Class<String[]> fromType() { return String[].class; }
  public Class<TransferMethod[]> toType() { return TransferMethod[].class; }
}