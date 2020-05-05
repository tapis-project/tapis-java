package edu.utexas.tacc.tapis.systems.dao;

import org.jooq.Converter;
import org.jooq.EnumType;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

public class TransferMethodArrayConverter implements Converter<EnumType[], TransferMethod[]>
{
  public TransferMethod[] from(EnumType[] ea)
  {
    TransferMethod[] ta = {};
    return ta;
  }

  public EnumType[] to(TransferMethod[] ta)
  {
    EnumType[] ea = {};
    return ea;
  }

  public Class<EnumType[]> fromType() { return EnumType[].class; }
  public Class<TransferMethod[]> toType() { return TransferMethod[].class; }
}