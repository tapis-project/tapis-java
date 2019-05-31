package edu.utexas.tacc.tapis.shared.threadlocal;

import java.util.Stack;

/** This class allows access to thread local context information during 
 * request processing.  During REST requests, the thread local information
 * is initialized in the calling API code.  The calling API code is also
 * responsible for removing the context after the library method called from
 * the API code completes.
 * 
 * By defining the thread local context information in a separate class any
 * code that properly initializes a context object can call library methods.
 * For example, test code that is not tied to an HTTP request can synthesize
 * a context object before calling any backend library method.  For example,
 * here's how to set the tenantId in a test program:
 * 
 *      AloeThreadContext context = AloeThreadLocal.aloeThreadContext.get();
 *      context.setTenantId("iplantc.org");
 *
 * 
 * @author rcardone
 */
public final class AloeThreadLocal 
{
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // Stack of context objects.
  private static final Stack<AloeThreadContext> _stack = new Stack<>();
  
  // The single thread local field that contains all per thread job context information.
  // The get() call on this field will cause a new context object to initialize the
  // calling thread's private context if the thread is new or if remove() had previously
  // been called by this thread.
  public static ThreadLocal<AloeThreadContext> aloeThreadContext =
	    ThreadLocal.<AloeThreadContext>withInitial(() -> {return new AloeThreadContext();});
	
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* push:                                                                        */
  /* ---------------------------------------------------------------------------- */
	/** Push the current context on a stack and return the new context, which is a
	 * clone of the pushed context.
	 * 
	 * @return the cloned version of the previous context which is now the current context
	 * @throws CloneNotSupportedException on clone failure
	 */
	public static AloeThreadContext push() 
	 throws CloneNotSupportedException
	{
	  _stack.push(aloeThreadContext.get());
	  aloeThreadContext.set((AloeThreadContext) aloeThreadContext.get().clone());
	  return aloeThreadContext.get();
	}

  /* ---------------------------------------------------------------------------- */
  /* pop:                                                                         */
  /* ---------------------------------------------------------------------------- */
	/** Pop the context on the top of the stack, make it the current context and
	 * return it.
	 * 
	 * @return the new current context which was popped off the stack
	 */
	public static AloeThreadContext pop()
	{
	    aloeThreadContext.set(_stack.pop());
	  return aloeThreadContext.get();
	}
}
