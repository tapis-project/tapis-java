package edu.utexas.tacc.tapis.shared.utils;

import java.util.ArrayList;
import java.util.Hashtable;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.shared.utils.SimpleTimer;

@Test(groups={"unit"})
public class SimpleTimerTest 
{
    /* **************************************************************************** */
    /*                               Constants and Fields                           */
    /* **************************************************************************** */
    private static final int ITERATIONS = 50;
    
    /* **************************************************************************** */
    /*                                    Tests                                     */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* sequentialTimerTest:                                                         */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void sequentialTimerTest() throws InterruptedException
    {   
        // 10 ms sleep.
        SimpleTimer st = SimpleTimer.start("firstPeriod");
        System.out.println(st.getStartMsg());
        Thread.sleep(10);
        System.out.println(st.getStopMsg());
        Assert.assertTrue(st.getElapsedSeconds() >= .01d,
                          "Unexpected elapsed time for " + st.getLabel());
        
        // 30 ms sleep.
        st = SimpleTimer.start("secondPeriod");
        Thread.sleep(30);
        Assert.assertTrue(st.getElapsedSeconds() >= .03d,
                          "Unexpected elapsed time for " + st.getLabel());
        
        // 1/2 second sleep
        st = SimpleTimer.start("thirdPeriod");
        Thread.sleep(500);
        System.out.println(st.getShortStopMsg());
        Assert.assertTrue(st.getElapsedSeconds() >= .5d, 
                          "Unexpected elapsed time for " + st.getLabel());
    }

    /* ---------------------------------------------------------------------------- */
    /* concurrentTimerTest:                                                         */
    /* ---------------------------------------------------------------------------- */
    /** Test that multiple threads running the same code get unique labels.
     * 
     * @throws InterruptedException
     */
    @Test(enabled=true)
    public void concurrentTimerTest() throws InterruptedException
    {   
        // Create the thread-related sets.
        final Hashtable<String,String> labels = new Hashtable<>(ITERATIONS * 2 + 1);
        ArrayList<Thread> threads = new ArrayList<>(ITERATIONS);
        
        // Spawn a number of threads that all start timers.
        for (int i = 0; i < ITERATIONS; i++) {
            
            Thread t = new Thread() {
                public void run() {
                    
                    // Pick the same label prefix for all threads
                    // to detect name collisions.
                    SimpleTimer st = SimpleTimer.start("childThread");
                    if (labels.put(st.getLabel(), "x") != null) 
                        System.out.println(st.getLabel() + "already exists!");
                    
                    // Sleep so that we register some duration greater than 0.
                    try {Thread.sleep(10);}
                        catch (InterruptedException e) {}
                    Assert.assertTrue(st.getElapsedSeconds() >= .01d,
                            "Unexpected elapsed time for " + st.getLabel());
                }
            };
            threads.add(t);
            t.start();
        }
        
        // Wait for all the threads to complete.
        for (Thread t : threads) t.join(10000); // set a maximum timeout
        
        // Make sure there are the expected number of unique labels
        // created by the child threads.
        Assert.assertEquals(labels.size(), ITERATIONS, 
                            "Expected " + ITERATIONS + " unique labels,"
                                    + " but only " + labels.size() + " were created.");
    }
}
