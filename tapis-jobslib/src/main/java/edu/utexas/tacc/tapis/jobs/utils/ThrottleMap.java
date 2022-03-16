package edu.utexas.tacc.tapis.jobs.utils;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class provides a concurrent map of string keys to Throttle object values.
 * 
 * @author rcardone
 */
public final class ThrottleMap 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(ThrottleMap.class);
    
    // The cleaner thread wakes up interval in seconds.
    private static final int CLEANER_SLEEP_SECONDS = 120; // 2 minutes.
    
    // Initial number of buckets in throttles map.
    private static final int INITIAL_CAPACITY = 31;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The name of this map.
    private final String _name;
    
    // The sliding time window duration that we track in seconds.
    private final int    _throttleSeconds;
    
    // The maximum number of insertions allowed in the time window.
    private final int    _throttleLimit;
    
    // Number of milliseconds that the cleaner thread sleeps.
    private final int    _cleanerSleepMs;
    
    // The repository of insertion times in descending order (latest to earliest).
    private final ConcurrentHashMap<String,Throttle> _throttles;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Construct a map of throttles, each one throttling a different resource.
     * Use the default cleaner thread sleep time and initial map capacity.
     * 
     * @param name of this map
     * @param throttleSeconds the number of seconds in the sliding window
     * @param throttleLimit the maximum number of times record() can be called in the window
     */
    public ThrottleMap(String name, int throttleSeconds, int throttleLimit)
    {this(name, throttleSeconds, throttleLimit, CLEANER_SLEEP_SECONDS, INITIAL_CAPACITY);}

    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Construct a map of throttles, each one throttling a different resource.
     * 
     * @param name of this map
     * @param throttleSeconds the number of seconds in the sliding window
     * @param throttleLimit the maximum number of times record() can be called in the window
     * @param cleanerSeconds the number of seconds the cleaner thread sleeps
     * @param initCapacity the initial capacity of the throttles map
     */
    public ThrottleMap(String name, int throttleSeconds, int throttleLimit, 
                       int cleanerSeconds, int initCapacity)
    {
        // Set this object's read-only values.
        _name = name;
        _throttleSeconds = throttleSeconds;
        _throttleLimit   = throttleLimit;
        _cleanerSleepMs  = cleanerSeconds * 1000;
        
        // Create the map of throttles.
        _throttles = new ConcurrentHashMap<String,Throttle>(initCapacity);
        
        // Start the cleaner thread to remove unneeded key/value pairs from the map.
        startCleanerThread();
    }

    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public int getSeconds(){return _throttleSeconds;}
    public int getLimit(){return _throttleLimit;}
    public ConcurrentHashMap<String,Throttle> getThrottles(){return _throttles;}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* record:                                                                */
    /* ---------------------------------------------------------------------- */
    /** Select or insert the key's throttle and add a new time record to the 
     * throttle's list if the limit hasn't been exceeded.
     * 
     * @return true if a record was added, false if the limit was exceeded and
     *         no record was added
     */
    public boolean record(String key)
    {
        // Get the associated throttle if it exists.
        var throttle = _throttles.get(key);
        if (throttle == null) throttle = addThrottle(key);
        return throttle.record();
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addThrottle:                                                           */
    /* ---------------------------------------------------------------------- */
    private Throttle addThrottle(String key)
    {
        // putIfAbsent is atomic so two threads won't step on each other.
        // If the key is inserted by another thread before putIfAbsent's
        // synchronized code block is entered, then the previous throttle
        // is returned and we discard the new one.
        var throttle = new Throttle(_throttleSeconds, _throttleLimit);
        var prevThrottle = _throttles.putIfAbsent(key, throttle);
        return prevThrottle != null ? prevThrottle : throttle;
    }
    
    /* ---------------------------------------------------------------------- */
    /* startCleanerThread:                                                    */
    /* ---------------------------------------------------------------------- */
    private void startCleanerThread()
    {
        // Use the map name as the threadgroup name.
        var cleaner = new ThrottleMapCleaner(_name, "cleanerThread");
        cleaner.setDaemon(true);
        cleaner.start();
    }

    /* ********************************************************************** */
    /*                         ThrottleMapCleaner Class                       */
    /* ********************************************************************** */
    private final class ThrottleMapCleaner
     extends Thread
    {
        // Set of candidate keys to remove.  The first time a throttle is
        // discovered to be empty, its key is added to this set.  If on the
        // next cleaning round the throttle continues to be empty, we assume
        // that it has stabilized, it actually is empty and it can be removed
        // from the _throttles map. 
        final private HashSet<String> _candidateKeys = new HashSet<>();
        
        // Constructor.
        private ThrottleMapCleaner(String groupName, String threadName) 
        {super(new ThreadGroup(groupName), threadName);}
        
        // Remove empty map entries.
        @Override
        public void run()
        {
            // Announce our existence.
            _log.info(MsgUtils.getMsg("JOBS_THROTTLEMAP_STARTING", _name, 
                                      _throttleSeconds, _throttleLimit, 
                                      _cleanerSleepMs));
            
            // Processing loop.
            while(true) {
                // Sleep the configured amount of time.
                try {Thread.sleep(_cleanerSleepMs);}
                    catch (Exception e) {return;}
                
                // Find the empty throttles and use a 2 step process for 
                // removing an entry from the map.  When a throttle is
                // empty twice in a row, it will be removed from the map.
                var it = _throttles.entrySet().iterator();
                while (it.hasNext()) {
                    // Get the next key and throttle.
                    var entry = it.next();
                    var key   = entry.getKey();
                    
                    // Demote from candidate set if throttle has entries.
                    if (!entry.getValue().isEmpty()) {
                        _candidateKeys.remove(key);
                        continue;
                    }
                    
                    // If the throttle is already a condidate for 
                    // removal, then remove it from the map and
                    // the candidate set.
                    if (_candidateKeys.contains(key)) {
                        it.remove();
                        _candidateKeys.remove(key);
                    } 
                    else 
                        // Promote to candidate set
                        _candidateKeys.add(key); 
                }
            }
        }
    }
}
