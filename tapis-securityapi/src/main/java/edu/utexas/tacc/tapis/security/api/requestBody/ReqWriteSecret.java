package edu.utexas.tacc.tapis.security.api.requestBody;

import java.util.Map;

public final class ReqWriteSecret
{
    public Options options;
    public Map<String,String> data;
    
    public static final class Options
    {
        public int cas;
    }
}
