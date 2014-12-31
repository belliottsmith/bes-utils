package bes.concurrent.order;

import bes.concurrent.WaitQueue;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Represents a group of identically ordered operations, i.e. all operations started in the interval between
 * two barrier issuances. For each register() call this is returned, close() must be called exactly once.
 * It should be treated like taking a lock().
 */
public final class Phase implements Comparable<Phase>
{

    /**
     * Constant to which Phase.running is set when complete
     */
    private static final int FINISHED = 1 << 31;
    private static final int BLOCKING = 1 << 30;
    private static final int RUNNING = BLOCKING - 1;

    /**
     * In general this class goes through the following stages:
     * 1) LIVE:      many calls to register() and close()
     * 2) FINISHING: a call to expire() (after a barrier issue), means calls to register() will now fail,
     *               and we are now 'in the past' (new operations will be started against a new Ordered)
     * 3) FINISHED:  once the last close() is called, this Ordered is done. We call unlink().
     * 4) ZOMBIE:    all our operations are finished, but some operations against an earlier Ordered are still
     *               running, or tidying up, so unlink() fails to remove us
     * 5) COMPLETE:  all operations started on or before us are FINISHED (and COMPLETE), so we are unlinked
     *
     * Two other parallel states are SAFE and ISBLOCKING:
     *
     * safe => all running operations were paused, so safe wrt memory access. Like a java safe point, except
     * that it only provides ordering guarantees, as the safe point can be exited at any point. The only reason
     * we require all to be stopped is to avoid tracking which threads are safe at any point, though we may want
     * to do so in future.
     *
     * isBlocking => a barrier that is waiting on us (either directly, or via a future Ordered) is blocking general
     * progress. This state is entered by calling Barrier.markBlocking(). If the running operations are blocked
     * on a Signal that is also registered with the isBlockingSignal (probably through isSafeBlockingSignal)
     * then they will be notified that they are blocking forward progress, and may take action to avoid that.
     */

    volatile Phase prev, next;
    private final long id; // monotonically increasing id for compareTo()

    // b63: is finishing
    // b62: is blocking
    // b31-62: unsafe-count
    // b0-31: in-progress-count
    private volatile int state = 0; // number of operations currently in progress, and number safe
    final WaitQueue waiting = new WaitQueue(); // signal to wait on for completion

    static final AtomicIntegerFieldUpdater<Phase> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(Phase.class, "state");

    // constructs first instance only
    Phase()
    {
        this.id = 0;
    }

    Phase(Phase prev)
    {
        this.id = prev.id + 1;
        this.prev = prev;
    }

    // prevents any further operations starting against this Phase
    // if there are no running operations, calls unlink; otherwise, we let the last op to close call it.
    // this means issue() won't have to block for ops to finish.
    void expire()
    {
        while (true)
        {
            int current = state;
            assert current >= 0;
            int next = FINISHED | current;
            if (stateUpdater.compareAndSet(this, current, next))
            {
                // if we're already finished (no running ops), unlink ourselves
                if (isFinished(next))
                    unlink();
                return;
            }
        }
    }

    // attempts to start an operation against this Ordered instance, and returns true if successful.
    boolean register()
    {
        while (true)
        {
            int current = state;
            if (current < 0)
                return false;
            assert current + 1 < 1 << 30;
            if (stateUpdater.compareAndSet(this, current, current + 1))
                return true;
        }
    }


    /**
     * To be called exactly once for each register() call this object is returned for, indicating the operation
     * is complete
     */
    void closeOne()
    {
        assert next == null || next.prev == this;
        while (true)
        {
            int current = state;
            int next = (current & RUNNING) - 1;
            next |= (current & ~RUNNING);
            if (stateUpdater.compareAndSet(this, current, next))
            {
                if (isFinished(next))
                    unlink();
                return;
            }
        }
    }

    /**
     * called exactly once per Phase, once we know all operations started against it have completed.
     * We do not know if operations against its ancestors have completed, or if its descendants have
     * completed ahead of it, so we attempt to create the longest chain from the oldest still linked
     * Phase. If we can't reach the oldest through an unbroken chain of FINISHED Phase, we abort, and
     * leave the still completing ancestor(s) to tidy up (and signal us).
     */
    private void unlink()
    {
        // unlink any FINISHED (not nec. COMPLETE) nodes directly behind us
        // we only modify ourselves in this step, not anybody behind us, so we don't have to worry about races
        // this is equivalent to walking the list, but potentially makes life easier for any unlink operations
        // that proceed us if we aren't yet COMPLETE
        // note we leave the forward (next) chain fully intact at this stage
        Phase start = this;
        Phase prev = this.prev;
        while (prev != null)
        {
            // if we hit an unfinished ancestor, we're not COMPLETE, so leave it to the ancestor to clean up when done
            if (!prev.isFinished())
                return;
            start = prev;
            this.prev = prev = prev.prev;
        }

        // our waiters are good to go, so signal them
        this.waiting.signalAll();

        // now walk from earliest to latest, unlinking pointers as we go to tidy up for GC, and waking up any blocking threads
        // we don't stop with the list before us though, as we may have finished late, so once we've destroyed the list
        // behind us we carry on until we hit a node that isn't FINISHED
        // we can safely abort if we ever hit null, as any unlink that finished before us would have been completely
        // unlinked by its prev pointer before we started, so we'd never visit it, so it must have been running
        // after we called unlink, by which point we were already marked FINISHED, so it would tidy up just as well as we intend to
        while (true)
        {
            Phase next = start.next;
            start.next = null;
            start.waiting.signalAll();
            if (next == null)
                return;
            next.prev = null;
            if (!next.isFinished())
                return;
            start = next;
        }
    }

    private static boolean isFinished(long state)
    {
        return (state & ~BLOCKING) == FINISHED;
    }

    public boolean isFinished()
    {
        return isFinished(state);
    }

    public boolean isComplete()
    {
        return prev == null && isFinished();
    }

    /**
     * @return true if a barrier we are behind is, or may be, blocking general progress,
     * so we should try more aggressively to progress
     */
    public boolean isBlocking()
    {
        return (state & BLOCKING) != 0;
    }

    public void markBlocking()
    {
        for (int current = state;
             !stateUpdater.compareAndSet(this, current, BLOCKING | current) ;
             current = state);
        waiting.signalAll();
    }

    public int compareTo(Phase that)
    {
        // we deliberately use subtraction, as opposed to Long.compareTo() as we care about ordering
        // not which is the smaller value, so this permits wrapping in the unlikely event we exhaust the long space
        long c = this.id - that.id;
        if (c > 0)
            return 1;
        else if (c < 0)
            return -1;
        else
            return 0;
    }

    public boolean happensBefore(Barrier barrier)
    {
        Phase that = barrier.happensAfter;
        if (that == null)
            return true;
        return that.id - id >= 0;
    }
}
