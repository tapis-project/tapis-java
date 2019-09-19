package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;

public class ClientSessionCacheLoader extends CacheLoader<String, String> {

  @Override
  public String load(String key) {
    return key.toLowerCase();
  }

}
