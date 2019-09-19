package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class ClientSessionCacheRemovalListener implements RemovalListener<String, String> {

  @Override
  public void onRemoval(RemovalNotification<String, String> removalNotification) {
    removalNotification.getValue().toLowerCase();
  }
}
