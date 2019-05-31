package edu.utexas.tacc.tapis.sharedapi.servlet.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.SessionCookieConfig;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/** This servlet logging filter is configured for use by individual projects by
 * subclassing this class or by using this class directly.  The first level of 
 * filtering takes place using the WebFilter annotation that the subclass defines.  
 * This request filter can use "urlPatterns" or "servletNames" annotation
 * attributes to restrict the requests that get logged. 
 *  
 * A second level of filtering takes place using an environment variable that 
 * lists the uri prefixes that are configured for logging. For example, if the 
 * environment variable EnvVar.TAPIS_REQUEST_LOGGING_FILTER_PREFIXES contains 
 * "/jobs/v2" as an entry, then all requests whose uris begin with that string will 
 * be logged by this class.  This second level filtering is provided to allow 
 * dynamic control of logging without restarting the servlet.   
 * 
 * 
 * The ordering of filters, which cannot be specified using annotations, is 
 * specified in the web.xml file of this project.  
 * 
 * @author rcardone
 *
 */
@WebFilter(filterName = "TapisLoggingFilter", urlPatterns = "*")
public class TapisLoggingFilter
 implements Filter
{
	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(TapisLoggingFilter.class);
	
	// Initial buffer capacity.
	private static final int BUF_LEN = 1024;
	
	// The MDC key name.
	private static final String MDC_ID_KEY = TapisConstants.MDC_ID_KEY; 
	
	// Request/response correlation. At any given moment
	// this value represents the number of requests filtered.
	private static final AtomicLong _correlationId = new AtomicLong(0);

	/* **************************************************************************** */
	/*                                Public Methods                                */
	/* **************************************************************************** */
	/* ---------------------------------------------------------------------------- */
	/* init:                                                                        */
	/* ---------------------------------------------------------------------------- */
	@Override
	public void init(FilterConfig filterConfig) throws ServletException 
	{
		if (_log.isDebugEnabled()) 
			_log.debug(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVLET_FILTER", this.getClass().getName()));
	}

	/* ---------------------------------------------------------------------------- */
	/* doFilter:                                                                    */
	/* ---------------------------------------------------------------------------- */
	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException 
	{
		// ------------------ Dynamic  Filtering -------------------------
	    // Determine if logging has been turned on for this request by
	    // comparing the request path to path prefixes that have been specified
	    // in the uri filter environment variable.  The URI begins with a /,
	    // so / can be used as the wildcard prefix that turns on logging for
	    // all requests.  To log all job requests, specify /jobs/v2. 
	    HttpServletRequest httpRequest = null;
	    if (request instanceof HttpServletRequest)
	        httpRequest = (HttpServletRequest) request;
	  
	    // Select the path that determines if filtering will take place based on request type.
	    // The http uri is often a more specific (i.e., longer) path than the context path.
	    String filterPath;
	    if (httpRequest == null) {
	        // Use shorter context path.
	        filterPath = request.getServletContext().getContextPath();
	        
	        // Assign a random thread local id.
	        MDC.put(MDC_ID_KEY, TapisUtils.getRandomString());
	    }
	    else {
	        // Use the possibly longer path.
	        filterPath = httpRequest.getRequestURI();
	        
	        // See if the caller set the unique id. If not, use a random one.
	        final String requestId = httpRequest.getHeader(MDC_ID_KEY);
            if (StringUtils.isNotEmpty(requestId)) MDC.put(MDC_ID_KEY, requestId);
              else MDC.put(MDC_ID_KEY, TapisUtils.getRandomString());
	    }
	  
	    // Determine if we are filtering.
	    if (!TapisEnv.inEnvVarListPrefix(EnvVar.TAPIS_REQUEST_LOGGING_FILTER_PREFIXES, filterPath))
	    {
	        chain.doFilter(request, response);
	        return;
	    }
    
	    // Get a (for-all-practical-purposes) unique id to correlate request and response.
	    long correlationId = _correlationId.incrementAndGet();
    
	    // ---------------------- Request Processing ---------------------
	    // Dump request information.
	    StringBuilder buf = new StringBuilder(BUF_LEN);
	    buf.append("\n================ Servlet Request ");
	    buf.append(correlationId);
	    buf.append(" ================\n");
	    readServletRequest(request, buf);
	    if (httpRequest != null) readHttpServletRequest((HttpServletRequest)request, buf);
	    buf.append("============== End Servlet Request "); 
	    buf.append(correlationId);
	    buf.append(" ================\n");
	    _log.info(buf.toString());
	    buf = null; // allow gc
    
	    // ---------------------- Chain Processing -----------------------
	    // Call other filters.
	    chain.doFilter(request, response);
    
	    // ---------------------- Response Processing --------------------
	    // Dump response information.
	    buf = new StringBuilder(BUF_LEN);
	    buf.append("\n================ Servlet Response ");
	    buf.append(correlationId);
	    buf.append(" ================\n");
	    readServletResponse(response, buf);
	    if (response instanceof HttpServletResponse)
	        readHttpServletResponse((HttpServletResponse)response, buf);
	    buf.append("============== End Servlet Response ");
	    buf.append(correlationId);
	    buf.append(" ================\n");
	    _log.info(buf.toString());
	    
	    // Delete the MDC thread local value.
	    MDC.remove(MDC_ID_KEY);
	}

	/* ---------------------------------------------------------------------------- */
	/* destroy:                                                                     */
	/* ---------------------------------------------------------------------------- */
	@Override
	public void destroy(){}

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* readServletRequest:                                                          */
  /* ---------------------------------------------------------------------------- */
	/** Capture servlet request information.
	 * 
	 * @param request the source request
	 * @param buf the output buffer
	 */
	private void readServletRequest(ServletRequest request, StringBuilder buf)
	{
	  // ------------------ ServletRequest Processing ------------------
    buf.append("NETWORK: ");
    buf.append("serverName=");
    buf.append(request.getServerName());
    buf.append(", ");
    buf.append("serverPort=");
    buf.append(request.getServerPort());
    buf.append(", ");

    buf.append("localName=");
    buf.append(request.getLocalName());
    buf.append(", ");
    buf.append("localAddr=");
    buf.append(request.getLocalAddr());
    buf.append(", ");
    buf.append("localPort=");
    buf.append(request.getLocalPort());
    buf.append(", ");

    buf.append("remoteHost=");
    buf.append(request.getRemoteHost());
    buf.append(", ");
    buf.append("remoteAddr=");
    buf.append(request.getRemoteAddr());
    buf.append(", ");
    buf.append("remotePort=");
    buf.append(request.getRemotePort());
    buf.append("\n");
    
    buf.append("ENCODING: ");
    buf.append("characterEncoding=");
    buf.append(request.getCharacterEncoding());
    buf.append(", ");
    buf.append("contentLength=");
    buf.append(request.getContentLength());
    buf.append(", ");
    buf.append("contentType=");
    buf.append(request.getContentType());
    buf.append(", ");
    buf.append("locale=");
    buf.append(request.getLocale().toString());
    buf.append(", ");
    buf.append("contentLength=");
    buf.append(request.getContentLength());
    buf.append(", ");
    buf.append("isSecure=");
    buf.append(request.isSecure());
    buf.append(", ");
    buf.append("scheme=");
    buf.append(request.getScheme());
    buf.append("\n");
    
    buf.append("ASYNC: ");
    buf.append("asyncSupport=");
    buf.append(request.isAsyncSupported());
    buf.append(", ");
    buf.append("asyncStarted=");
    buf.append(request.isAsyncStarted());
    buf.append("\n");
    
    // Servlet context.
    ServletContext context = request.getServletContext();
    buf.append("SERVLET CTX: ");
    buf.append("servletInfo=");
    buf.append(context.getServerInfo());
    buf.append(", ");
    buf.append("servletVersion=");
    buf.append(context.getEffectiveMajorVersion());
    buf.append(".");
    buf.append(context.getEffectiveMinorVersion());
    buf.append(", ");
    buf.append("servletName=");
    buf.append(context.getServletContextName());
    buf.append(", ");
    buf.append("servletVirtualName=");
    buf.append(context.getVirtualServerName());
    buf.append(", ");
    buf.append("contextPath=");
    buf.append(context.getContextPath());
    buf.append("\n");
    
    // Servlet cookie info.
    SessionCookieConfig cookieConfig = context.getSessionCookieConfig();
    buf.append("COOKIE CONFIG: ");
    buf.append("cookieMaxAge=");
    buf.append(cookieConfig.getMaxAge());
    buf.append(", ");
    buf.append("cookieName=");
    buf.append(cookieConfig.getName());
    buf.append(", ");
    buf.append("cookieDomain=");
    buf.append(cookieConfig.getDomain());
    buf.append(", ");
    buf.append("cookiePath=");
    buf.append(cookieConfig.getPath());
    buf.append("\n");
    
    // Context init parameters.
    Enumeration<String> initParms = context.getInitParameterNames(); 
    if (initParms.hasMoreElements()) {
      // Sort names.
      List<String> initList = Collections.list(initParms);
      Collections.sort(initList);

      // Output values.
      boolean first = true;
      buf.append("INIT PARMS: [");
      for (String initName : initList){
        if (!first) buf.append(", ");
          else first = false;
        buf.append(initName);
        buf.append("=");
        buf.append(context.getInitParameter(initName));
      }
      buf.append("]\n");
    }

    // Servlet parameters.
    Enumeration<String> params = request.getParameterNames(); 
    if (params.hasMoreElements()) {
      // Sort names.
      List<String> paramsList = Collections.list(params);
      Collections.sort(paramsList);
      
      // Output values.
      boolean first = true;
      buf.append("PARAMETERS: [");
      for (String paramName : paramsList){
        if (!first) buf.append(", ");
          else first = false;
        buf.append(paramName);
        buf.append("=");
        
        // A parameter can have multiple values.
        String[] values = request.getParameterValues(paramName);
        if (values != null) {
          boolean first2 = true;
          for (String value : values) {
            if (!first2) buf.append(", ");
              else first2 = false;
            buf.append(value);
          }
        }
      }
      buf.append("]\n");
    }
    
    // Servlet attributes.
    Enumeration<String> attrs = request.getAttributeNames(); 
    if (attrs.hasMoreElements()) {
      // Sort names.
      List<String> attrList = Collections.list(attrs);
      Collections.sort(attrList);
      
      // Output values.
      boolean first = true;
      buf.append("ATTRIBUTES: [");
      for (String attrName : attrList){
        if (!first) buf.append(", ");
          else first = false;
        buf.append(attrName);
        buf.append("=");
        buf.append(request.getAttribute(attrName));
      }
      buf.append("]\n");
    }
	}

  /* ---------------------------------------------------------------------------- */
  /* readHttpServletRequest:                                                      */
  /* ---------------------------------------------------------------------------- */
  /** Capture Http servlet request information.
   * 
   * @param request the source request
   * @param buf the output buffer
   */
  private void readHttpServletRequest(HttpServletRequest httpRequest, StringBuilder buf)
  {
    // ---------------- HttpServletRequest Processing ----------------
    // Http info
    buf.append("HTTP PATH: ");
    buf.append("contextPath=");
    buf.append(httpRequest.getContextPath());
    buf.append(", ");
    buf.append("servletPath=");
    buf.append(httpRequest.getServletPath());
    buf.append(", ");
    buf.append("pathInfo=");
    buf.append(httpRequest.getPathInfo());
    buf.append(", ");
    buf.append("pathTranslated=");
    buf.append(httpRequest.getPathTranslated());
    buf.append("\n");
    
    buf.append("HTTP QUERY STRING: ");
    buf.append("queryString=");
    buf.append(httpRequest.getQueryString());
    buf.append("\n");

    buf.append("HTTP RESOURCE: ");
    buf.append("requestURI=");
    buf.append(httpRequest.getRequestURI());
    buf.append(", ");
    buf.append("requestURL=");
    buf.append(httpRequest.getRequestURL());
    buf.append("\n");
    
    buf.append("HTTP USER: ");
    buf.append("requestedSessionId=");
    buf.append(httpRequest.getRequestedSessionId());
    buf.append(", ");
    buf.append("remoteUser=");
    buf.append(httpRequest.getRemoteUser());
    buf.append("\n");
    
    // Http headers.
    Enumeration<String> headerNames = httpRequest.getHeaderNames();
    if (headerNames.hasMoreElements()) {
      // Sort names.
      List<String> headerList = Collections.list(headerNames);
      Collections.sort(headerList);
      
      // Output values.
      buf.append("HTTP HEADERS: ");
      boolean first = true;
      for (String headerName : headerList) {
        // Avoid logging security information unless the 
        // controlling environment variable is set to true.
        if (!TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_LOG_SECURITY_INFO))
          if (headerName.toLowerCase().startsWith("x-jwt-assertion")) continue;
        
        // Allow this header key/value pair.
        if (!first) buf.append(", ");
          else first = false;
        buf.append(headerName);
        buf.append("=");
        
        // We expect at least one value for this header.
        Enumeration<String> headerValues = httpRequest.getHeaders(headerName);
        ArrayList<String> valueList = Collections.list(headerValues);
        boolean first2 = true;
        for (String value : valueList) {
          if (!first2) buf.append(", ");
            else first2 = false;
          buf.append(value);
        }
      }
      buf.append("\n");
    }
     
    // Http cookies.
    Cookie[] cookies = httpRequest.getCookies();
    if (cookies != null && cookies.length > 0) {
      buf.append("HTTP COOKIES: ");
      boolean first = true;
      for (Cookie cookie : cookies) {
        if (!first) buf.append(", "); 
          else first = false;
        buf.append(cookie.getName());
        buf.append("=");
        buf.append(cookie.getValue());
        buf.append(", ");
        buf.append("domain=");
        buf.append(cookie.getDomain());
        buf.append(", ");
        buf.append("path=");
        buf.append(cookie.getPath());
        buf.append(", ");
        buf.append("secure=");
        buf.append(cookie.getSecure());
      }
      buf.append("\n");
    }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* readServletResponse:                                                         */
  /* ---------------------------------------------------------------------------- */
  /** Capture servlet response information.
   * 
   * @param request the source response
   * @param buf the output buffer
   */
  private void readServletResponse(ServletResponse response, StringBuilder buf)
  {
    // ------------------ ServletResponse Processing -----------------
    buf.append("SERVLET RESPONSE: ");
    buf.append("bufferSize=");
    buf.append(response.getBufferSize());
    buf.append(", ");
    buf.append("charEncoding=");
    buf.append(response.getCharacterEncoding());
    buf.append(", ");
    buf.append("contentType=");
    buf.append(response.getContentType());
    buf.append(", ");
    buf.append("locale=");
    buf.append(response.getLocale());
    buf.append("\n");
  }

  /* ---------------------------------------------------------------------------- */
  /* readHttpServletResponse:                                                     */
  /* ---------------------------------------------------------------------------- */
  /** Capture Http servlet response information.
   * 
   * @param request the source response
   * @param buf the output buffer
   */
  private void readHttpServletResponse(HttpServletResponse httpResponse, StringBuilder buf)
  {
    // ---------------- HttpServletResponse Processing ---------------
    buf.append("HTTP RESPONSE: ");
    buf.append("status=");
    buf.append(httpResponse.getStatus());
    buf.append(" (");
    buf.append(Response.Status.fromStatusCode(httpResponse.getStatus()).getReasonPhrase());
    buf.append(")\n");
    
    // Http headers.
    Collection<String> headerNames = httpResponse.getHeaderNames();
    if (!headerNames.isEmpty()) {
      // Sort names.
      ArrayList<String> headerList = new ArrayList<String>(headerNames);
      Collections.sort(headerList);
      
      // Output values.
      buf.append("HTTP HEADERS: ");
      boolean first = true;
      for (String headerName : headerList) {
        if (!first) buf.append(", ");
          else first = false;
        buf.append(headerName);
        buf.append("=");
        
        // We expect at least one value for this header.
        Collection<String> headerValues = httpResponse.getHeaders(headerName);
        boolean first2 = true;
        for (String value : headerValues) {
          if (!first2) buf.append(", ");
            else first2 = false;
          buf.append(value);
        }
      }
      buf.append("\n");
    }
  }
}
