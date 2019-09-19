package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;

import java.util.concurrent.TimeUnit;

public class ClientSessionCache {

  private static CacheLoader loader = new ClientSessionCacheLoader();
  private static RemovalListener listener = new ClientSessionCacheRemovalListener();
  private static Cache<String, String> sessionCache =  CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
        .removalListener(listener)
        .build();
  private static final ClientSessionCache instance = new ClientSessionCache();

  private ClientSessionCache () {}


  public static Cache getInstance() {
    return sessionCache;
  }

  public static String get(String key){
    return sessionCache.getIfPresent(key);
  }

}
