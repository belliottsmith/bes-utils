/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package bes.concurrent;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import bes.concurrent.collections.AbstractSimpleCollection;

/**
 * <p>A relatively easy to use utility for general purpose thread signalling.</p>
 * <p>Usage on a thread awaiting a state change using a WaitQueue q is:</p>
 * <pre>
 * {@code
 *      while (!conditionMet())
 *          Signal s = q.register();
 *              if (!conditionMet())    // or, perhaps more correctly, !conditionChanged()
 *                  s.await();
 *              else
 *                  s.cancel();
 * }
 * </pre>
 * A signalling thread, AFTER changing the state, then calls q.signal() to wake up one, or q.signalAll()
 * to wake up all, waiting threads.
 * <p>To understand intuitively how this class works, the idea is simply that a thread, once it considers itself
 * incapable of making progress, registers to be awoken once that changes. Since this could have changed between
 * checking and registering it checks the condition again, suspending only if it hasn't changed/still is not met.</p>
 * <p>This thread synchronisation scheme has some advantages over Condition objects and Object.wait/notify in that no
 * monitor acquisition is necessary (except for the Thread signal, in rare race scenarios) and, in fact, besides the
 * actual waiting on a signal, all operations are non-blocking.
 *
 * As a result consumers can never block producers, nor each other, nor vice versa, from making progress.
 * Threads that are signalled are also put into a RUNNABLE state as simultaneously as the unpark calls permit,
 * so they can  all immediately make progress without having to serially acquire the monitor/lock, reducing the total
 * delay incurred.</p>
 *
 * <p>A few notes on utilisation:</p>
 * <p>1. A thread will only exit await() when it has been signalled, but this does not guarantee the condition has not
 * been altered since it was signalled, and depending on your design it is likely the outer condition will need to be
 * checked in a loop, though this is not always the case.</p>
 * <p>2. Each signal is single use, so must be re-registered after each await(). This is true even if it times out.</p>
 * <p>3. If you choose not to wait on the signal (because the condition has been met before you waited on it)
 * you must cancel() the signal if the signalling thread uses signal() to awake waiters; otherwise signals will be
 * lost. If signalAll() is used but infrequent, and register() is frequent, cancel() should still be used to prevent the
 * queue growing unboundedly. Similarly, if you provide a TimerContext, cancel should be used to ensure it is not erroneously
 * counted towards wait time.</p>
 * <p>4. Care must be taken when selecting conditionMet() to ensure we are waiting on the condition that actually
 * indicates progress is possible. In some complex cases it may be tempting to wait on a condition that is only indicative
 * of local progress, not progress on the task we are aiming to complete, and a race may leave us waiting for a condition
 * to be met that we no longer need.
 * <p>5. This scheme is not fair</p>
 * <p>6. Only the thread that calls register() may call await()</p>
 */
public final class WaitQueue extends AbstractSimpleCollection<WaitQueue.RegisteredSignal>
{

    private static final int CANCELLED = -1;
    private static final int SIGNALLED = 1;
    private static final int NOT_SET = 0;

    private static final Function<RegisteredSignal, Action> FIRST_UNSET = (s) -> s.isSet() ? Action.CONTINUE : Action.SUCCEED;
    private static final Function<RegisteredSignal, Action> FIRST_SIGNAL = (s) -> s.signal() ? Action.SUCCEED : Action.CONTINUE;
    private static final Function<RegisteredSignal, Action> SIGNAL = (s) -> { s.signal() ; return Action.REMOVE; };

    /**
     * The calling thread MUST be the thread that uses the signal
     * @return a new Signal registered with this queue
     */
    public Signal register()
    {
        RegisteredSignal insert = new RegisteredSignal();
        add(insert);
        return insert;
    }

    /**
     * Signal one waiting thread
     */
    public boolean signal()
    {
        return _visit(FIRST_SIGNAL) == Result.SUCCESS;
    }

    public boolean hasWaiters()
    {
        return _visit(FIRST_UNSET) == Result.SUCCESS;
    }

    /**
     * Signal all waiting threads
     */
    public void signalAll()
    {
        _visit(SIGNAL);
    }

    /**
     * A Signal is a one-time-use mechanism for a thread to wait for notification that some condition
     * state has transitioned that it may be interested in (and hence should check if it is).
     * It is potentially transient, i.e. the state can change in the meantime, it only indicates
     * that it should be checked, not necessarily anything about what the expected state should be.
     *
     * Signal implementations should never wake up spuriously, they are always woken up by a
     * signal() or signalAll().
     *
     * This abstract definition of Signal does not need to be tied to a WaitQueue.
     * Whilst RegisteredSignal is the main building block of Signals, this abstract
     * definition allows us to compose Signals in useful ways. The Signal is 'owned' by the
     * thread that registered itself with WaitQueue(s) to obtain the underlying RegisteredSignal(s);
     * only the owning thread should use a Signal.
     */
    public static interface Signal
    {

        /**
         * @return true if signalled; once true, must be discarded by the owning thread.
         */
        public boolean isSignalled();

        /**
         * atomically: cancels the Signal if !isSet(), or returns true if isSignalled()
         *
         * @return true if isSignalled()
         */
        public boolean checkAndClear();

        /**
         * Should only be called by the owning thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        public abstract void cancel();

        /**
         * Wait, without throwing InterruptedException, until signalled. On exit isSignalled() must be true.
         * If the thread is interrupted in the meantime, the interrupted flag will be set.
         */
        public void awaitUninterruptibly();

        /**
         * Wait until signalled, or throw an InterruptedException if interrupted before this happens.
         * On normal exit isSignalled() must be true; however if InterruptedException is thrown isCancelled()
         * will be true.
         * @throws InterruptedException
         */
        public void await() throws InterruptedException;

        /**
         * Wait until signalled, or the provided time is reached, or the thread is interrupted. If signalled,
         * isSignalled() will be true on exit, and the method will return true; if timedout, the method will return
         * false and isCancelled() will be true; if interrupted an InterruptedException will be thrown and isCancelled()
         * will be true.
         * @param nanos System.nanoTime() to wait until
         * @return true if signalled, false if timed out
         * @throws InterruptedException
         */
        public boolean awaitUntil(long nanos) throws InterruptedException;
    }

    /**
     * An abstract signal implementation
     */
    public static abstract class AbstractSignal implements Signal
    {
        public void awaitUninterruptibly()
        {
            WaitQueue.awaitUninterruptibly(this);
        }
        public void await() throws InterruptedException
        {
            WaitQueue.await(this);
        }
        public boolean awaitUntil(long nanos) throws InterruptedException
        {
            return WaitQueue.awaitUntil(this, nanos);
        }
    }

    /**
     * A signal registered with this WaitQueue
     */
    final class RegisteredSignal extends AbstractSimpleCollection.Node implements Signal
    {
        volatile Thread thread = Thread.currentThread();
        volatile int state;
        volatile RegisteredSignal next, prev;

        public boolean isSignalled()
        {
            return state == SIGNALLED;
        }

        public boolean isCancelled()
        {
            return state == CANCELLED;
        }

        public boolean isSet()
        {
            return state != NOT_SET;
        }

        private boolean signal()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, SIGNALLED))
            {
                Thread thread = this.thread;
                LockSupport.unpark(thread);
                this.thread = null;
                // don't call remove, as this happens at head of list
                return true;
            }
            return false;
        }

        public boolean checkAndClear()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                threadUpdater.lazySet(this, null);
                remove();
                return false;
            }
            // must now be signalled assuming correct API usage
            return true;
        }

        /**
         * Should only be called by the registered thread. Indicates the signal can be retired,
         * and, if already signalled, propagates the signal to another waiting thread
         */
        public void cancel()
        {
            if (isCancelled())
                return;
            if (!signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                // must already be signalled - switch to cancelled and
                signalledUpdater.lazySet(this, CANCELLED);
                // propagate the signal
                WaitQueue.this.signal();
            }
            threadUpdater.lazySet(this, null);
            remove();
        }

        public void awaitUninterruptibly()
        {
            WaitQueue.awaitUninterruptibly(this);
        }

        public void await() throws InterruptedException
        {
            WaitQueue.await(this);
        }

        public boolean awaitUntil(long nanos) throws InterruptedException
        {
            return WaitQueue.awaitUntil(this, nanos);
        }

        protected boolean isDeleted()
        {
            return isSet();
        }

        protected Node delete()
        {
            throw new UnsupportedOperationException();
        }
    }

    // for testing
    boolean present(Iterator<Signal> signals)
    {
        if (!signals.hasNext())
            return true;
        Function<RegisteredSignal, Action> f = new Function<RegisteredSignal, Action>()
        {
            Signal test = signals.next();
            public Action apply(RegisteredSignal s)
            {
                if (test != s)
                    return Action.CONTINUE;
                if (!signals.hasNext())
                    return Action.SUCCEED;
                test = signals.next();
                return Action.CONTINUE;
            }
        };
        return _visit(f) == Result.SUCCESS;
    }

    public static void awaitUninterruptibly(Signal s)
    {
        boolean interrupted = false;
        while (!s.isSignalled())
        {
            if (Thread.interrupted())
                interrupted = true;
            LockSupport.park();
        }
        if (interrupted)
            Thread.currentThread().interrupt();
        s.checkAndClear();
    }

    public static void await(Signal s) throws InterruptedException
    {
        while (!s.isSignalled())
        {
            checkInterrupted(s);
            LockSupport.park();
        }
        s.checkAndClear();
    }

    public static boolean awaitUntil(Signal s, long until) throws InterruptedException
    {
        long now;
        while (until > (now = System.nanoTime()) && !s.isSignalled())
        {
            checkInterrupted(s);
            long delta = until - now;
            LockSupport.parkNanos(delta);
        }
        return s.checkAndClear();
    }

    private static void checkInterrupted(Signal s) throws InterruptedException
    {
        if (Thread.interrupted())
        {
            s.cancel();
            throw new InterruptedException();
        }
    }

    private static final AtomicIntegerFieldUpdater<RegisteredSignal> signalledUpdater = AtomicIntegerFieldUpdater.newUpdater(RegisteredSignal.class, "state");
    private static final AtomicReferenceFieldUpdater<RegisteredSignal, Thread> threadUpdater = AtomicReferenceFieldUpdater.newUpdater(RegisteredSignal.class, Thread.class, "thread");

}
