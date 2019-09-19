package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.TimeUnit;

public class ClientSessionCache {

  private static Cache<String, String> sessionCache;
  private static ClientSessionCacheLoader loader;

  public ClientSessionCache () {

    loader = new ClientSessionCacheLoader();
    sessionCache =  CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build();
  }

  public static String get(String key) {
    return sessionCache.getIfPresent(key);
  }

}
