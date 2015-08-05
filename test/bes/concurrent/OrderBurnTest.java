package bes.concurrent;
/*
 * 
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
 * 
 */


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import bes.concurrent.collections.BroadcastQueue;
import bes.concurrent.collections.BroadcastView;
import bes.concurrent.order.Barrier;
import bes.concurrent.order.Op;
import bes.concurrent.order.Order;
import bes.concurrent.order.Phase;

import static org.junit.Assert.assertTrue;

public class OrderBurnTest
{

    static final int CONSUMERS = Runtime.getRuntime().availableProcessors();
    static final int PRODUCERS = CONSUMERS;

    static final long RUNTIME = TimeUnit.MINUTES.toMillis(5);
    static final long REPORT_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    static final boolean EXPOSE_BARRIER = false;

    static final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler()
    {
        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
            System.err.println(t.getName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    };

    final Order ordering = new Order();
    final AtomicInteger errors = new AtomicInteger();

    class TestOrdering implements Runnable
    {

        final ScheduledExecutorService sched;
        volatile State state = new State();
        volatile AtomicInteger running = new AtomicInteger();

        TestOrdering(ExecutorService exec, ScheduledExecutorService sched)
        {
            this.sched = sched;
            for (int i = 0 ; i < Math.max(1, PRODUCERS / CONSUMERS) ; i++)
                exec.execute(new Producer());
            exec.execute(this);
        }

        @Override
        public void run()
        {
            final long until = System.currentTimeMillis() + RUNTIME;
            long lastReport = System.currentTimeMillis();
            long count = 0;
            Phase min = null;
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (true)
            {
                long now = System.currentTimeMillis();
                if (now > until)
                    break;
                if (now > lastReport + REPORT_INTERVAL)
                {
                    lastReport = now;
                    System.out.println(String.format("Executed %d barriers. %.0f%% complete.",
                                                     count, 100 * (1 - ((until - now) / (double) RUNTIME))));
                }
                final State s = state;
                final Barrier barrier = ordering.newBarrier();
                if (EXPOSE_BARRIER)
                {
                    s.barrier = barrier;
                }
                s.replacement = new State();
                AtomicInteger unfinished = running;
                running = new AtomicInteger();
                barrier.issue();
                if (rnd.nextFloat() < 0.05f)
                {
                    while (!barrier.allPriorPhasesHaveFinished());
                }
                else
                {
                    barrier.await();
                }
                state = s.replacement;
                if (unfinished.get() != 0)
                {
                    errors.incrementAndGet();
                    System.err.println("Operations were running that should have all finished");
                }
                s.check(barrier, min);
                final Phase expectMin = min;
                sched.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        s.check(barrier, expectMin);
                    }
                }, 1, TimeUnit.SECONDS);
                count++;
                min = barrier.happensAfter();
            }
        }

        class State
        {

            volatile Barrier barrier;
            volatile State replacement;
            AtomicReference<Phase> max = new AtomicReference<>();
            AtomicReference<Phase> min = new AtomicReference<>();
            AtomicInteger running = new AtomicInteger();

            boolean accept(Phase group)
            {
                if (EXPOSE_BARRIER)
                {
                    if (barrier != null && !barrier.happensAfter(group))
                        return false;
                    running.incrementAndGet();
                    while (true)
                    {
                        Phase curmax = max.get();
                        if (curmax != null && curmax.compareTo(group) >= 0)
                            break;
                        if (max.compareAndSet(curmax, group))
                            break;
                    }
                    running.decrementAndGet();
                }
                while (true)
                {
                    Phase curmin = min.get();
                    if (curmin != null && curmin.compareTo(group) <= 0)
                        break;
                    if (min.compareAndSet(curmin, group))
                        break;
                }
                return true;
            }

            void check(Barrier barrier, Phase expectMin)
            {
                if (EXPOSE_BARRIER)
                {
                    if (running.get() != 0)
                    {
                        errors.incrementAndGet();
                        System.err.println("Operations were running that should have all finished (2)");
                    }
                    if (max.get() != null && max.get().compareTo(barrier.happensAfter()) > 0)
                    {
                        errors.incrementAndGet();
                        System.err.println("We got an operation from the future");
                    }
                }
                if (expectMin != null && min.get() != null && min.get().compareTo(expectMin) < 0)
                {
                    errors.incrementAndGet();
                    System.err.println("We got an operation from the past");
                }
            }

        }

        class Producer implements Runnable
        {
            public void run()
            {
                while (true)
                {
                    try (Op op = ordering.start())
                    {
                        State s = state;
                        AtomicInteger counter = running;
                        counter.incrementAndGet();
                        while (!s.accept(op.phase()))
                            s = s.replacement;
                        counter.decrementAndGet();
                    }
                }
            }
        }

    }

    class TestResourceClearing
    {

        final class Resource implements Runnable
        {
            volatile Runnable run = this;
            public void run() { }
        }

        final BroadcastQueue<Resource> read = new BroadcastQueue<>();
        final BroadcastView<Resource> clean = read.view();

        TestResourceClearing(ExecutorService exec)
        {
            final Semaphore raceAhead = new Semaphore(50000);
            exec.execute(new Producer(raceAhead));
            exec.execute(new Consumer());
            exec.execute(new Cleaner(raceAhead));
        }

        class Producer implements Runnable
        {
            final Semaphore raceAhead;
            Producer(Semaphore raceAhead)
            {
                this.raceAhead = raceAhead;
            }

            public void run()
            {
                try
                {
                    while (true)
                    {
                        for (int i = 0 ; i < 1000 ; i++)
                            read.append(new Resource());
                        while (true)
                        {
                            raceAhead.acquire(1000);
                            for (int i = 0 ; i < 1000 ; i++)
                            {
                                read.advance();
                                read.append(new Resource());
                            }
                        }
                    }
                }
                catch (InterruptedException e)
                {
                    throw new IllegalStateException();
                }
            }
        }

        class Consumer implements Runnable
        {
            public void run()
            {
                while (true)
                {
                    AtomicInteger c;
                    try (Op op = ordering.start())
                    {
                        for (Resource item : read)
                            item.run.run();
                    }
                }
            }
        }

        class Cleaner implements Runnable
        {
            final Semaphore raceAhead;
            Cleaner(Semaphore raceAhead)
            {
                this.raceAhead = raceAhead;
            }

            public void run()
            {
                List<Resource> collect = new ArrayList<>(10000);
                while (true)
                {
                    int count = clean.drainTo(collect, 10000);
                    int offset = 0;
                    for (int i = 0 ; i < count ; i++)
                    {
                        if (collect.get(i) == read.peek())
                        {
                            collect(collect, offset, i);
                            offset = i;
                            while (collect.get(i) == read.peek());
                        }
                    }
                    collect(collect, offset, count);
                    collect.clear();
                }
            }
            private void collect(List<Resource> collect, int offset, int end)
            {
                Barrier barrier = ordering.newBarrier();
                barrier.issue();
                barrier.await();
                for (int i = offset ; i < end ; i++)
                {
                    assert collect.get(i).run != null;
                    collect.get(i).run = null;
                }
                raceAhead.release(end - offset);
            }
        }

    }

    @Test
    public void testOrdering() throws InterruptedException
    {
        errors.set(0);
        Thread.setDefaultUncaughtExceptionHandler(handler);
        final ExecutorService exec = Executors.newCachedThreadPool();
        final ScheduledExecutorService checker = Executors.newScheduledThreadPool(1);
        for (int i = 0 ; i < CONSUMERS ; i++)
            new TestOrdering(exec, checker);
        exec.shutdown();
        exec.awaitTermination((long) (RUNTIME * 1.1), TimeUnit.MILLISECONDS);
        assertTrue(exec.isShutdown());
        assertTrue(errors.get() == 0);
    }

    @Test
    public void testResourceClearing() throws InterruptedException
    {
        errors.set(0);
        Thread.setDefaultUncaughtExceptionHandler(handler);
        final ExecutorService exec = Executors.newCachedThreadPool();
        for (int i = 0 ; i < PRODUCERS ; i++)
            new TestResourceClearing(exec);
        exec.shutdown();
        exec.awaitTermination((long) (RUNTIME * 1.1), TimeUnit.MILLISECONDS);
        assertTrue(exec.isShutdown());
        assertTrue(errors.get() == 0);
    }


}
