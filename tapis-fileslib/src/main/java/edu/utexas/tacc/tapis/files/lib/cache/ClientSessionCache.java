package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;

import java.util.concurrent.TimeUnit;

public class ClientSessionCache {

  private static Cache<String, String> sessionCache;
  private CacheLoader loader;
  private RemovalListener listener;

  public ClientSessionCache () {

    loader = new ClientSessionCacheLoader();
    listener = new ClientSessionCacheRemovalListener();
    sessionCache =  CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .removalListener(listener)
        .build();
  }


}
