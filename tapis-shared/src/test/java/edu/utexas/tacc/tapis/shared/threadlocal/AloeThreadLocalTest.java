package edu.utexas.tacc.tapis.shared.threadlocal;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.threadlocal.AloeThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.AloeThreadLocal;

@Test(groups={"unit"})
public class AloeThreadLocalTest 
{
	/* **************************************************************************** */
	/*                                    Tests                                     */
	/* **************************************************************************** */
	/* ---------------------------------------------------------------------------- */
	/* test1:                                                                       */
	/* ---------------------------------------------------------------------------- */
	/** Make sure thread local contexts are initialized for 2 threads.
	 * 
	 * @throws InterruptedException
	 */
	@Test(enabled=true)
	public void test1() throws InterruptedException
	{
		Thread thread1 = new Thread(new MyRunnable(1));
		Thread thread2 = new Thread(new MyRunnable(2));
		
	    thread1.start();
	    thread2.start();

	    thread1.join(); 
	    thread2.join(); 
	}
	
	/* ---------------------------------------------------------------------------- */
	/* test2:                                                                       */
	/* ---------------------------------------------------------------------------- */
	/** Set the tenantId for each thread and make sure that the value returned
	 * from the thread local context is the initialized value.
	 * 
	 * @throws InterruptedException
	 */
	@Test(enabled=true)
	public void test2() throws InterruptedException
	{
		Thread thread1 = new Thread(new MyRunnable(1, "tenant1"));
		Thread thread2 = new Thread(new MyRunnable(2, "tenant2"));
		
	    thread1.start();
	    thread2.start();

	    thread1.join(); 
	    thread2.join();
	}
	
  /* ---------------------------------------------------------------------------- */
  /* test3:                                                                       */
  /* ---------------------------------------------------------------------------- */
	/** Test the thread local context stack.
	 *  
	 * @throws CloneNotSupportedException on clone failure
	 */
  @Test(enabled=true)
  public void test3() throws CloneNotSupportedException
  {
    // Get the original context.
    AloeThreadContext context1 = AloeThreadLocal.aloeThreadContext.get();
    context1.setTenantId("tenant1");
    AloeThreadContext context2 = AloeThreadLocal.push();
    Assert.assertTrue(context2 == AloeThreadLocal.aloeThreadContext.get(),
                        "Expected current context to be the same object returned from push().");
    Assert.assertEquals(context2.getTenantId(), context1.getTenantId(), 
                        "Expected cloned context tenantId to match original.");
    
    // Change the current tenantId.
    context2.setTenantId("tenant2");
    Assert.assertEquals(AloeThreadLocal.aloeThreadContext.get().getTenantId(), "tenant2", 
                        "Expected current context tenantId to have changed.");
    
    // Push the current context.
    AloeThreadContext context3 = AloeThreadLocal.push();
    Assert.assertTrue(context3 == AloeThreadLocal.aloeThreadContext.get(),
        "Expected current context to be the same object returned from push().");
    Assert.assertEquals(context3.getTenantId(), context2.getTenantId(), 
        "Expected cloned context tenantId to match second context.");
    
    // Change the current tenantId.
    context3.setTenantId("tenant3");
    Assert.assertEquals(AloeThreadLocal.aloeThreadContext.get().getTenantId(), "tenant3", 
                        "Expected current context tenantId to have changed.");
    
    // Pop last context.
    AloeThreadContext pop2 = AloeThreadLocal.pop();
    Assert.assertTrue(context2 == pop2,
        "Expected popped context to be the last pushed context.");
    Assert.assertTrue(pop2 == AloeThreadLocal.aloeThreadContext.get(),
        "Expected popped context to be the current context.");
    Assert.assertEquals(pop2.getTenantId(), "tenant2", 
        "Expected current context tenantId to the second tenantId.");
    
    // Pop last context.
    AloeThreadContext pop1 = AloeThreadLocal.pop();
    Assert.assertTrue(context1 == pop1,
        "Expected popped context to be the first pushed context.");
    Assert.assertTrue(pop1 == AloeThreadLocal.aloeThreadContext.get(),
        "Expected popped context to be the current context.");
    Assert.assertEquals(pop1.getTenantId(), "tenant1", 
        "Expected current context tenantId to be the first tenantId.");
  }
  
	/* **************************************************************************** */
	/*                                   Classes                                    */
	/* **************************************************************************** */
	private static class MyRunnable 
	 implements Runnable
	 {
		// Fields
		private int id;
		private String tenantId;
		
		// Constructors
		private MyRunnable(int id) {this.id = id;}
		private MyRunnable(int id, String tenantId) {this.id = id; this.tenantId = tenantId;}
		
        @Override
        public void run() {
        	AloeThreadContext context = AloeThreadLocal.aloeThreadContext.get();
        	if (tenantId != null) context.setTenantId(tenantId);
    
        	// Get the thread local tenant id.
        	Assert.assertEquals(context.getTenantId(), 
        			            (tenantId == null) ? AloeThreadContext.INVALID_ID : tenantId,
        	                    "Unexpected tenantId value on thread " + id);
            
        	// Removing the thread local context should cause it 
        	// to be reinitialized on the next get call.
            AloeThreadLocal.aloeThreadContext.remove();
            context = AloeThreadLocal.aloeThreadContext.get();
        	Assert.assertEquals(context.getTenantId(), 
		                        AloeThreadContext.INVALID_ID,
                                "Expected the default tenantId value on thread: " + id);
        }
		
	 }
}
