package edu.utexas.tacc.tapis.shared.schema;

import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;
import org.everit.json.schema.loader.SchemaClient;
import org.everit.json.schema.loader.internal.DefaultSchemaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class provides a schema client that intercepts calls to the default client
 * to load Java resource files.  The default client handles the http(s) and file
 * protocols, but the file protocol requires an absolute path.  The default client
 * also uses the json schema's $id value as the base URL with which it resolves
 * all $ref URLs.  The default approach is not practical for use with resource
 * files that reside in relocateable directory subtrees.  This class allows schemas
 * defined in resource files in a subtree to reference each other using the usual
 * Class.getResourceAsStream() search rules.
 * 
 * @author rcardone
 */
public class ResourceSchemaClient 
  implements SchemaClient
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(ResourceSchemaClient.class);
  
  // Protocols we handle.
  private static final String RESOURCE_PROTOCOL = "resource://";
  
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // The client that handles all non-resource protocols.  The class's get() method
  // is reentrant, so reusing the a single instance is not a problem.
  private static final DefaultSchemaClient _defaultClient = new DefaultSchemaClient();
  
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* get:                                                                         */
  /* ---------------------------------------------------------------------------- */
  @Override
  public InputStream get(String url) 
  {
    // Check input.
    if (url == null) {
      _log.error(MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "ResourceSchemaClient", "get"));
      return null;
    }
    
    // See if the protocol is one we handle.
    if (!url.startsWith(RESOURCE_PROTOCOL)) return _defaultClient.get(url);
    
    // Return an input stream for the resource protocol.
    String resource = StringUtils.removeStart(url, RESOURCE_PROTOCOL);
    InputStream ins = getClass().getResourceAsStream(resource);
    if (ins == null) _log.error(MsgUtils.getMsg("TAPIS_RESOURCE_NOT_FOUND", resource));
    return ins;
  }
}
