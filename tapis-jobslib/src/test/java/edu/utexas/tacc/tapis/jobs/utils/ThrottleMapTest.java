package edu.utexas.tacc.tapis.jobs.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups={"unit"})
public class ThrottleMapTest 
{
    @Test
    public void throttleMapTest1() throws Exception
    {
        // --------------  Typical output --------------
        // true
        // true
        // false
        // false
        // false
        // 2022-02-16 14:20:10.854 INFO  [cleanerThread/] e.u.t.t.j.u.ThrottleMap$ThrottleMapCleaner:167 - JOBS_THROTTLEMAP_STARTING Cleaner thread in ThrottleMap "test1" starting with sleeptime=2,000 ms, throttleSeconds=2, throttleLimit=2.
        // 2
        // 0
        // 0
        // 0
        // ---------------------------------------------
        
        // The output is very dependent on the following settings, which means
        // that the assertions need to be tweaked whenever the settings change.
        // Since the ThrottleMap starts its own cleaner thread, timing and, 
        // ultimately, whether the test passes depends on the speed of execution.
        
        // Create a custom ThrottleMap with short cleaner sleep periods so that
        // we can force the unused Throttle object to be removed from the map.
        // The Throttle has a small capacity and a short sliding window so that
        // it will a) quickly exceed its capacity and b) quickly recover its
        // capacity.
        final int throttleSeconds = 2;
        final int throttleLimit   = 2;
        final int cleanerSeconds  = 2;
        final int initCapacity    = 7;
        var tmap = new ThrottleMap("test1", throttleSeconds, throttleLimit, 
                                   cleanerSeconds, initCapacity);
        
        // Try to add more than the number that will fit.
        for (int i = 0; i < 5; i++) {
            var b = tmap.record("key1");
            System.out.println(b);
            boolean expected = i > 1 ? false : true;
            Assert.assertEquals(expected, b, "Unexpected iteration " + i + " record return value.");
        }
        
        // Test for enough time for the sliding window of the throttle to be empty
        // and for cleaner thread to remove the throttle from the map.
        for (int i = 0; i < 6; i++) {
            Thread.sleep(1000);
            var throttle = tmap.getThrottles().get("key1");
            if (throttle == null) break;
            throttle.removeExpiredElements();
            var len = throttle.getQueueLength();
            System.out.println(len);
            int expected = i > 0 ? 0 : 2;
            Assert.assertEquals(expected, len, "Unexpected iteration " + i + " record return value.");
        }
        
        // The throttle should have been removed.
        Assert.assertNull(tmap.getThrottles().get("key1"));
    }
}
