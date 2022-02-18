package edu.utexas.tacc.tapis.jobs.utils;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/** This class maintains a history of insertion times that can signify any event
 * that a caller wants to limit within a specified time period.  For example, 
 * each time a thread is restarted, a call to insert a timestamp can be made on 
 * this class.  The insert will succeed if the maximum number of inserts would 
 * not be exceeded within the configured, sliding time period.
 * 
 * This class is constructed with a duration in seconds that defines the sliding
 * window period and a limit of the number of times record() can be called
 * within that window.
 * 
 * Design Note
 * -----------
 * The use of a concurrent double ended queue (dequeue) without further 
 * synchronization does not guarantee that the sequence of timestamps are in perfect
 * descending order from head to tail.  The removeExpiredElements() assumes that
 * timestamps later in time appear closer to the head, but this may not be true
 * when multiple threads call record() at nearly the same time.  
 * 
 * The actual consequence of out of order timestamps should not be significant since
 * race conditions would place temporally close timestamps near each other in the
 * dequeue.  removeExpiredElements() might not remove all possible timestamps
 * on a given call, but would with all likelihood make up for it on its next
 * invocation.  The use of an ordered set such as a ConcurrentSkipListSet would 
 * avoid this phenomenon but incur a higher insertion cost.      
 * 
 * @author rcardone
 */
public final class Throttle
{
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The sliding time window duration that we track in seconds.
    private final int _seconds;
    
    // The maximum number of insertions allowed in the time window.
    private final int _limit;
    
    // The repository of insertion times in descending order (latest to earliest).
    private final ConcurrentLinkedDeque<Instant> _times;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Construct a throttle that indicates whether a limit has been exceeded
     * in the most recent time period start now and extending "seconds" into 
     * the past.
     * 
     * @param seconds the number of seconds in the sliding window
     * @param limit the maximum number of times record() can be called in the window
     */
    public Throttle(int seconds, int limit)
    {
        _seconds = seconds;
        _limit = limit;
        _times = new ConcurrentLinkedDeque<Instant>();
    }

    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public int getSeconds(){return _seconds;}
    public int getLimit(){return _limit;}
    public int getQueueLength() {return _times.size();} // Beware of overhead 
    public boolean isEmpty() {return _times.isEmpty();}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* record:                                                                */
    /* ---------------------------------------------------------------------- */
    /** Add a new time record to the list if the limit hasn't been exceeded.
     * 
     * @return true if a record was added, false if the limit was exceeded and
     *         no record was added
     */
    public boolean record()
    {
        // Remove all timestamps that fall outside the time window.
        removeExpiredElements();
        
        // Determine if there's room on the dequeue for another timestamp.
        if (_times.size() >= _limit) return false;
        
        // Insert an new timestamp.
        _times.addFirst(Instant.now());
        return true;
    }
    
    /* ********************************************************************** */
    /*                            Package Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* removeExpiredElements:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Remove time records that fall outside the sliding window. Package access
     * for testing.
     */
    void removeExpiredElements()
    {
        // Maybe there's nothing to check.
        if (_times.isEmpty()) return;
        
        // Get the current time.
        Instant oldest = Instant.now().minusSeconds(_seconds); 
        
        // Remove elements from the end of the queue (the oldest) 
        // until one is found within the configured window.  Given
        // that we insert at the head of the queue, once we find
        // an element within the window, all previous ones can be
        // assumed to be within the window, too.
        Iterator<Instant> it = _times.descendingIterator();
        while (it.hasNext()) {
            Instant curElement = it.next();
            if (curElement.isBefore(oldest)) it.remove();
             else break; // no need to check earlier timestamps.
        }
    }
}
