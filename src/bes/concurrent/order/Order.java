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
package bes.concurrent.order;

/**
 * <p>A class for providing synchronization between producers and consumers that do not
 * communicate directly with each other, but where the consumers need to process their
 * work in contiguous batches. In particular this is useful for both CommitLog and Memtable
 * where the producers (writing threads) are modifying a structure that the consumer
 * (flush executor) only batch syncs, but needs to know what 'position' the work is at
 * for co-ordination with other processes,
 *
 * <p>The typical usage is something like:
 * <pre>
     public final class ExampleShared
     {
        final OpOrder order = new OpOrder();
        volatile SharedState state;

        static class SharedState
        {
            volatile Barrier barrier;

            // ...
        }

        public void consume()
        {
            SharedState state = this.state;
            state.setReplacement(new State())
            state.doSomethingToPrepareForBarrier();

            state.barrier = order.newBarrier();
            // issue() MUST be called after newBarrier() else barrier.isAfter()
            // will always return true, and barrier.await() will fail
            state.barrier.issue();

            // wait for all producer work started prior to the barrier to complete
            state.barrier.await();

            // change the shared state to its replacement, as the current state will no longer be used by producers
            this.state = state.getReplacement();

            state.doSomethingWithExclusiveAccess();
        }

        public void produce()
        {
            try (Group opGroup = order.start())
            {
                SharedState s = state;
                while (s.barrier != null && !s.barrier.isAfter(opGroup))
                    s = s.getReplacement();
                s.doProduceWork();
            }
        }
    }
 * </pre>
 */
public class Order
{

    /**
     * A linked list starting with the most recent Phase object, i.e. the one we should start new operations from,
     * with (prev) links to any incomplete Phase instances, and (next) links to any potential future Phase instances.
     * Once all operations started against an Ordered instance and its ancestors have been finished the next instance
     * will unlink this one
     */
    volatile Phase current = new Phase();

    /**
     * Start an operation against this OpOrder.
     * Once the operation is completed Ordered.close() MUST be called EXACTLY once for this operation.
     *
     * @return the Ordered instance that manages this OpOrder
     */
    public Op start()
    {
        while (true)
        {
            Phase current = this.current;
            if (current.register())
                return new Op(current);
        }
    }

    /**
     * Start an operation that can be used for a longer running transaction, that periodically reaches points that
     * can be considered to restart the transaction. This is stronger than 'safe', as it declares that all guarded
     * entry points have been exited and will be re-entered, or an equivalent guarantee can be made that no reclaimed
     * resources are being referenced.
     *
     * <pre>
     * SyncingOrdered ord = startSync();
     * while (...)
     * {
     *     ...
     *     ord.sync();
     * }
     * ord.finish();
     *</pre>
     * is semantically equivalent to (but more efficient than):
     *<pre>
     * Ordered ord = start();
     * while (...)
     * {
     *     ...
     *     ord.close();
     *     ord = start();
     * }
     * ord.close();
     * </pre>
     */
    public MultiOp startMulti()
    {
        return new MultiOp(this);
    }

    /**
     * Creates a new barrier. The barrier is only a placeholder until barrier.issue() is called upon it,
     * after which all new operations will start against a new Group that will not be accepted
     * by barrier.happensAfter(), and barrier.await() will return only once all operations started prior to the issue
     * have completed.
     */
    public Barrier newBarrier()
    {
        return new Barrier(this);
    }

    public Phase getCurrent()
    {
        return current;
    }

}
