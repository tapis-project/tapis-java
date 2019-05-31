package edu.utexas.tacc.tapis.shared.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

/** A one-time use timer class that captures start and stop times and calculates 
 * elapsed time.  Each instance of this class generates a probablistically
 * unique label consisting of a short random string concatenated to a user-provided 
 * prefix string.  This generated label allows start and stop times to be correlated
 * in multithreaded environments.  
 * 
 * Here are examples of two common usages patterns.
 * 
 *  1. Start and stop message example.
 *      
 *      SimpleTimer st = SimpleTimer.start("DbCall1");
 *      System.out.println(st.getStartMsg());
 *      ...CALL to DB...
 *      System.out.println(st.getStopMsg());
 *      
 *      output: 
 *          DbCall1[FkAM] Start time is 2018-02-01T08:24:19.239.
 *          DbCall1[FkAM] Stop time is 2018-02-01T08:24:19.308 (0.069 seconds).
 *
 *  2. Slf4j logger message example.
 *  
 *      SimpleTimer st = null;
 *      if (log.isDebugEnabled()) st = SimpleTimer.start("DbCall2");
 *      ...CALL to DB...
 *      if (st != null) log.debug(st.getShortStopMsg());
 *      
 *      output:
 *          [2018-02-01 08:42:54.953] DEBUG Worker-13  SomeClass:101 - DbCall2[b9TO]: 0.5 seconds
 * 
 * @author rcardone
 */
public final class SimpleTimer 
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    // 3 bytes of randomness fit into 2^24 - 1. 
    private static final int CEILING = 0x1000000;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Single use timer fields.
    private final String  label;
    private final Instant startTime;
    private Instant       stopTime;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    private SimpleTimer(String prefix)
    {
        // Create a probablistically unique label and fix the start time. 
        this.label = prefix + "[" + getRandomString() + "]";
        this.startTime = Instant.now();
    }

    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* start:                                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Initialize a new timer using a prefix string that has meaning to the user.
     * Typically the prefix will uniquely identify the block of code that is to be
     * timed.  This method starts the timer. 
     * 
     * @param prefix string used to identify timer to caller
     * @return a new timer object whose start time has been set
     */
    public static SimpleTimer start(String prefix)
    {
        return new SimpleTimer(prefix);
    }

    /* ---------------------------------------------------------------------------- */
    /* stop:                                                                        */
    /* ---------------------------------------------------------------------------- */
    /** Stop the timer.  This method is idempotent; once a timer is stopped it cannot
     * be reused.  It's results can be retrieved any number of times.
     */
    public void stop()
    {
        if (stopTime == null) stopTime = Instant.now();
    }

    /* ---------------------------------------------------------------------------- */
    /* getStartMsg:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Generate a message based on the start time of this timer.
     * 
     * @return start time message
     */
    public String getStartMsg()
    {
        LocalDateTime localStartTime = LocalDateTime.ofInstant(startTime, ZoneId.systemDefault());
        return label + " Start time is " + localStartTime + ".";
    }

    /* ---------------------------------------------------------------------------- */
    /* getStopMsg:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Stop the timer and generate a stop message that includes the time the timer
     * was stopped.  This method is idempotent; the first time it is called, the timer 
     * is implicitly stopped.  Calls thereafter just regenerate the same message.
     * 
     * @return stop time message
     */
    public String getStopMsg()
    {
        // Stop the timer if necessary.
        stop();
        LocalDateTime localStopTime  = LocalDateTime.ofInstant(stopTime,  ZoneId.systemDefault());
        return label + " Stop time is " + localStopTime + 
                " (" + getDurationAfterStop() + " seconds)."; 
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getShortStopMsg:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Stop the timer and generate a minimal message that contains the timer's label
     * and the elapsed time in seconds.  This method is idempotent; the first time it 
     * is called, the timer is implicitly stopped.  Calls thereafter just regenerate 
     * the same message.
     * 
     * @return the short stop message
     */
    public String getShortStopMsg()
    {
        // Stop the timer if necessary.
        stop();
        return label + ": " + getDurationAfterStop() + " seconds"; 
    }
    /* ---------------------------------------------------------------------------- */
    /* getElapsedSeconds:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Stop the timer and return the raw number of elapsed seconds.  This method is 
     * idempotent; the first time it is called, the timer is implicitly stopped.  
     * Calls thereafter just return the same elapsed time.
     * 
     * @return the elapsed time in seconds
     */
    public double getElapsedSeconds()
    {
        // Stop the timer if necessary.
        stop();
        return getDurationAfterStop();
    }

    /* **************************************************************************** */
    /*                                 Accessors                                    */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getLabel:                                                                    */
    /* ---------------------------------------------------------------------------- */
    public String getLabel() {return label;}

    /* ---------------------------------------------------------------------------- */
    /* getStartTime:                                                                */
    /* ---------------------------------------------------------------------------- */
    public Instant getStartTime() {return startTime;}

    /* ---------------------------------------------------------------------------- */
    /* getStopTime:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public Instant getStopTime() {return stopTime;}
    
    /* **************************************************************************** */
    /*                                Private Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getRandomString:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Generate a pseudo-random base64url string that can be used to identify a 
     * timer for this thread.  
     * 
     * @return the randomized string
     */
    private static String getRandomString() 
    {
        // Get a pseudo-random int value that has its low-order 
        // 24 bits randomized, which is enough to generate a 
        // 4 character base64 string.
        int n = ThreadLocalRandom.current().nextInt(CEILING);
        byte[] b = new byte[3];
        b[2] = (byte) (n);
        n >>>= 8;
        b[1] = (byte) (n);
        n >>>= 8;
        b[0] = (byte) (n);
        
        // Encode the 3 bytes into 4 characters 
        // and avoid any padding.
        return Base64.getUrlEncoder().encodeToString(b);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getDurationAfterStop:                                                        */
    /* ---------------------------------------------------------------------------- */
    /** The timer must be stopped before this method is called.
     * 
     * @return the elapsed time in seconds between start and stop times
     */
    private double getDurationAfterStop()
    {
        return (stopTime.toEpochMilli() - startTime.toEpochMilli())/1000d;
    }
}
