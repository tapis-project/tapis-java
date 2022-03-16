package edu.utexas.tacc.tapis.jobs.utils;

import org.testng.annotations.Test;

/** A short demonstration of how sliding window throttling works.
 * 
 * @author rcardone
 */
@Test(groups={"unit"})
public class ThrottleTest 
{
    /** Appropriate to automatically run during compilation 
     * since it only waits for 1 second.
     */
    @Test
    public void throttleVeryShortTest()
    {
        // Set the parameters for the run.
        final int window = 1;
        final int limit  = 2;
        Throttle throttle = new Throttle(window, limit);
        final int waitms = 1000;
        final int total  = 3;
        int finished     = 0;
        
        // Throttle the number of records.
        while (finished < total) {
            if (throttle.record()) {
                print("Finished record " + ++finished + ".");
            }
            else {
                print("Waiting " + waitms + " milliseconds before attempting next record.");
                try {Thread.sleep(waitms);} catch (Exception e) {}
            }
        }
    }
    
    /** Disabled by default to avoid compilation delays.
     */
    @Test(enabled=false)
    public void throttleShortTest()
    {
        // Set the parameters for the run.
        final int window = 2;
        final int limit  = 2;
        Throttle throttle = new Throttle(window, limit);
        final int waitms = 1000;
        final int total  = 6;
        int finished     = 0;
        
        // Throttle the number of records.
        while (finished < total) {
            if (throttle.record()) {
                print("Finished record " + ++finished + ".");
            }
            else {
                print("Waiting " + waitms + " milliseconds before attempting next record.");
                try {Thread.sleep(waitms);} catch (Exception e) {}
            }
        }
    }
    
    private void print(String s) {System.out.println(s);}
}
