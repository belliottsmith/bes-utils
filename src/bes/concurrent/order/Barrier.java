package bes.concurrent.order;

import bes.concurrent.WaitQueue;

/**
 * This class represents a synchronisation point providing ordering guarantees on operations started
 * against the enclosing OpOrder.  When issue() is called upon it (may only happen once per Barrier), the
 * Barrier atomically partitions new operations from those already running (by expiring the current Group),
 * and activates its isAfter() method which indicates if an operation was started before or after this partition.
 * It offers methods to determine, or block until, all prior operations have finished, and a means to indicate
 * to those operations that they are blocking forward progress. See {@link Order} for idiomatic usage.
 */
public final class Barrier
{

    private final Order order;

    // this Barrier was issued after all Group operations started against happensAfter
    volatile Phase happensAfter;

    public Barrier(Order order) {
        this.order = order;
    }

    /**
     * @return true if @param group was started prior to the issuing of the barrier.
     *
     * (Until issue is called, always returns true, but if you rely on this behavior more than transiently,
     * between exposing the Barrier and calling issue() soon after, you are Doing It Wrong.)
     */
    public boolean happensAfter(Phase phase)
    {
        if (happensAfter == null)
            return true;
        // we subtract to permit wrapping round the full range of Long - so we only need to ensure
        // there are never Long.MAX_VALUE * 2 total Group objects in existence at any one time, which will
        // take care of itself
        return phase.compareTo(happensAfter) <= 0;
    }

    /**
     * Issues the barrier, meaning no new operations may be issued against it, and expires the current
     * Group.  Must be called after exposing the barrier for isAfter() to be properly synchronised, and before
     * any call to any other method on the Barrier.
     */
    public void issue()
    {
        if (happensAfter != null)
            throw new IllegalStateException("Can only call issue() once on each Barrier");

        final Phase current;
        synchronized (order)
        {
            current = order.current;
            happensAfter = current;
            order.current = current.next = new Phase(current);
        }
        current.expire();
    }

    /**
     * Mark all prior operations as blocking, potentially signalling them to more aggressively make progress
     */
    public void markBlocking()
    {
        Phase current = happensAfter;
        while (current != null)
        {
            current.markBlocking();
            current = current.prev;
        }
    }

    /**
     * Register to be signalled once allPriorPhasesHaveFinished() or allPriorOpsAreFinishedOrSafe() may return true
     */
    public WaitQueue.Signal register()
    {
        return happensAfter.waiting.register();
    }

    /**
     * @return true if all operations started prior to barrier.issue() have completed
     */
    public boolean allPriorPhasesHaveFinished()
    {
        Phase current = happensAfter;
        if (current == null)
            throw new IllegalStateException("This barrier needs to have issue() called on it before prior operations can complete");
        return current.isComplete();
    }

    /**
     * wait for all operations started prior to issuing the barrier to complete
     */
    public void await()
    {
        while (!allPriorPhasesHaveFinished())
        {
            WaitQueue.Signal signal = register();
            if (allPriorPhasesHaveFinished())
            {
                signal.cancel();
                break;
            }
            else
                signal.awaitUninterruptibly();
        }
    }

    /**
     * returns the Group we are waiting on - any Group with .compareTo(happensAfter()) <= 0
     * must complete before await() returns
     */
    public Phase happensAfter()
    {
        return happensAfter;
    }
}
