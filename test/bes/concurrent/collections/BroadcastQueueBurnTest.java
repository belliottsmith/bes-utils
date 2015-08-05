package bes.concurrent.collections;

import org.junit.*;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BroadcastQueueBurnTest
{

    final int THREAD_COUNT = 32;
    final int THREAD_MASK = THREAD_COUNT - 1;
    final ExecutorService exec = Executors.newFixedThreadPool(THREAD_COUNT);

    @Test
    public void testSafeIteratorRemoval() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval(true, 500, 1 << 14);
    }

    @Test
    public void testUnsafeIteratorRemoval() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval(false, 500, 1 << 14);
    }

    // all we care about is that iterator removals don't delete anything other than they target,
    // since it makes no guarantees about removal succeeding. To try and test it thoroughly we will
    // spin deleting until all our intended deletes complete successfully.
    // We also partially test snapshots, iterators and views.
    public void testIteratorRemoval(final boolean safe, final int batchCount, final int batchSize) throws ExecutionException, InterruptedException
    {

        final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicInteger totalBatchCount = new AtomicInteger();
        final AtomicLong failedDeletes = new AtomicLong();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            final int offset = i;
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    for (int batch = 0 ; batch < batchCount ; batch++)
                    {
                        final int end = offset + (THREAD_COUNT * batchSize);
                        BroadcastQueue.Snap<Integer> snap = queue.snap();
                        for (int i = offset ; i < end ; i+= THREAD_COUNT)
                            queue.append(i);

                        // check all my items are still there
                        snap = snap.extend();
                        int find = offset;
                        for (Integer v : snap)
                        {
                            if ((v & THREAD_MASK) == offset)
                            {
                                if (v != find)
                                {
                                    System.out.println(String.format("Unexpected next value (1); expected %s, found %s", find, v));
                                    return Boolean.FALSE;
                                }
                                find += THREAD_COUNT;
                            }
                        }
                        if (find != end)
                        {
                            System.out.println(String.format("Unexpected last value (1); expected %s, found %s", end, find));
                            return Boolean.FALSE;
                        }

                        // delete every other item, and loop until they're all gone, failing if we cannot delete more than
                        // 50% over 1000 tries
                        int tries = 0;
                        int notmissing = 0;
                        while (++tries <= 1000)
                        {
                            notmissing = 0;
                            find = offset;
                            AtomicIterator<Integer> iter = snap.iterator();
                            while (iter.hasNext() && find != end)
                            {
                                Integer next = iter.next();
                                if ((next & THREAD_MASK) == offset)
                                {
                                    if (next == find)
                                        find += THREAD_COUNT << 1;
                                    else if (next > find)
                                    {
                                        System.out.println(String.format("Unexpected next value (2) on try %s; expected %s, found %s", tries, find, next));
                                        return Boolean.FALSE;
                                    }
                                    else
                                    {
                                        if (safe)
                                            iter.atomicRemove();
                                        else
                                            iter.remove();
                                        notmissing++;
                                    }
                                }
                            }
                            if (find != end)
                            {
                                System.out.println(String.format("Unexpected last value (2) on try %s; expected %s, found %s", tries, end, find));
                                return Boolean.FALSE;
                            }
                            if (tries > 1 && notmissing < batchSize / 4)
                                break;
                        }
                        if (tries > 1000)
                        {
                            System.out.println(String.format("Failed to delete 50%% of items from the queue, despite 1000 tries (deleted %s of %s)", batchSize - notmissing, batchSize));
                            return Boolean.FALSE;
                        }
                        // poll the entire queue, to permit GC
                        while (queue.advance() != null);

                        // racey stats
                        int da = deleteAttempts.addAndGet(tries - 1);
                        long fd = failedDeletes.addAndGet(notmissing);
                        int bc = totalBatchCount.incrementAndGet();
                        if (bc % 1000 == 0)
                            System.err.println(String.format("Batch %s of %s Complete. %s%% transient deletes on average, after %s tries.", bc, batchCount * THREAD_COUNT, 100 * fd / ((batchSize / 2) * (double) bc), da / (double) bc));
                        if (safe && fd != 0)
                        {
                            System.out.println(String.format("Failed to delete some items, despite running safe deletes"));
                            return Boolean.FALSE;
                        }
                    }

                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    @Test
    public void testUnsafeIteratorRemoval2() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval2(false, 500, 1 << 16);
    }
    @Test
    public void testSafeIteratorRemoval2() throws ExecutionException, InterruptedException
    {
        testIteratorRemoval2(true, 500, 1 << 16);
    }
    // similar to testIteratorRemoval, except every thread operates over the same range to test hyper competitive deletes
    public void testIteratorRemoval2(final boolean safe, final int batchCount, final int batchSize) throws ExecutionException, InterruptedException
    {
        final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicInteger totalBatchCount = new AtomicInteger();
        final AtomicLong failedDeletes = new AtomicLong();
        final AtomicInteger deleteAttempts = new AtomicInteger();
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    for (int batch = 0 ; batch < batchCount ; batch++)
                    {
                        BroadcastQueue.Snap<Integer> snap = queue.snap();
                        for (int i = 0 ; i < batchSize ; i += 1)
                            queue.append(i);

                        // snap a range of the queue that should contain all the items we add
                        snap = snap.extend();
                        // delete every other item, and loop until they're all gone, failing if we cannot delete more than
                        // 50% over 1000 tries. Note that since we're operating over the same range as other operations here
                        // we delete every instance we see that isn't (mathematically) even
                        int tries = 0;
                        int notmissing = 0;
                        while (++tries <= 1000)
                        {
                            int find = 0;
                            notmissing = 0;
                            AtomicIterator<Integer> iter = snap.iterator();
                            while (iter.hasNext())
                            {
                                Integer next = iter.next();
                                if ((next & 1) == 1)
                                {
                                    if (safe)
                                        iter.atomicRemove();
                                    else
                                        iter.remove();
                                    notmissing++;
                                }
                                else if (next == find)
                                    find += 2;
                            }
                            if (find != batchSize)
                            {
                                System.out.println(String.format("Unexpected last value; expected %s, found %s", batchSize, find));
                                return Boolean.FALSE;
                            }
                            if (tries > 1 && notmissing < batchSize / 4)
                                break;
                        }
                        if (tries > 1000)
                        {
                            System.out.println("Failed to delete 50% of items from the queue, despite 1000 tries");
                            return Boolean.FALSE;
                        }
                        // poll the entire queue, to permit GC
                        while (queue.advance() != null);

                        int da = deleteAttempts.addAndGet(tries - 1);
                        long fd = failedDeletes.addAndGet(notmissing);
                        int bc = totalBatchCount.incrementAndGet();
                        if (bc % 1000 == 0)
                            System.err.println(String.format("Batch %s of %s Complete. %s%% transient deletes on average, after %s tries.", bc, batchCount * THREAD_COUNT, 100 * fd / ((batchSize / 2) * (double) bc), da / (double) bc));

                        if (safe && fd != 0)
                        {
                            System.out.println("Failed to delete some items, despite running safe deletes");
                            return Boolean.FALSE;
                        }
                    }

                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    @Test
    public void testSimpleAppendAndAdvance() throws ExecutionException, InterruptedException
    {
        final int batchSize = 1 << 20;
        final int batchCount = 20;
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
            final List<Future<int[]>> success = new ArrayList<>();
            for (int i = 0 ; i < THREAD_COUNT ; i++)
            {
                final int offset = i;
                success.add(exec.submit(new Callable<int[]>()
                {
                    @Override
                    public int[] call()
                    {
                        int[] items = new int[batchSize];
                        for (int i = 0 ; i < batchSize ; i++)
                        {
                            queue.append((i * THREAD_COUNT) + offset);
                            items[i] = queue.advance();
                        }
                        return items;
                    }
                }));
            }

            final boolean[] found = new boolean[batchSize * THREAD_COUNT];
            for (Future<int[]> result : success)
            {
                for (int i : result.get())
                {
                    Assert.assertFalse(found[i]);
                    found[i] = true;
                }
            }
            for (boolean b : found)
                Assert.assertTrue(b);
            Assert.assertTrue(queue.isEmpty());
            System.err.println(String.format("Batch %s of %s", batch + 1, batchCount));
        }
    }

    @Test
    public void testAdvanceAndIterate() throws ExecutionException, InterruptedException
    {
        final int batchSize = 1 << 24;
        final int batchCount = 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            final CountDownLatch latch = new CountDownLatch(rc * 2);
            for (int i = 0 ; i < rc ; i++)
            {
                final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
                queue.append(0);
                final AtomicInteger min = new AtomicInteger();
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (int i = 1 ; i < batchSize ; i++)
                        {
                            queue.append(i);
                            if (i >= 64)
                            {
                                queue.advance();
                                min.set(i - 63);
                            }
                        }
                        while (queue.advance() != null);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        while (!queue.isEmpty())
                        {
                            int bound = min.get();
                            for (Integer i : queue)
                            {
                                if (i < bound)
                                {
                                    errors.incrementAndGet();
                                    System.out.println(String.format("Error: saw a previously advanced item: %s vs %s", i, bound));
                                }
                            }
                        }
                        latch.countDown();
                    }
                });
            }
            latch.await();
            Assert.assertTrue(errors.get() == 0);
            System.err.println(String.format("Batch %s of %s", batch + 1, batchCount));
        }
    }

    @Test
    public void testPreciseIteratorRemovals() throws ExecutionException, InterruptedException
    {
        final int batchSize = 1 << 22;
        final int batchCount = 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = Math.max(1, Runtime.getRuntime().availableProcessors() / 3);
            final CountDownLatch latch = new CountDownLatch(rc * 3);
            final AtomicBoolean check = new AtomicBoolean(true);
            for (int i = 0 ; i < rc ; i++)
            {
                final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
                queue.append(0);
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (int i = 1 ; i < batchSize ; i++)
                            queue.append(i);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        while (check.get())
                            for (Integer i : queue);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            AtomicIterator<Integer> iter = queue.iterator();
                            for (int i = 0; i < batchSize; i++)
                            {
                                while (!iter.hasNext()) {
                                    while (queue.isEmpty());
                                    iter = queue.iterator();
                                }
                                int next = iter.next().intValue();
                                if (next != i)
                                {
                                    errors.incrementAndGet();
                                    System.out.println(String.format("expected %s, found %s ", i, next));
                                }
                                if (!iter.atomicRemove())
                                {
                                    errors.incrementAndGet();
                                    System.out.println("Failed to remove");
                                }
                            }
                            latch.countDown();
                            check.set(false);
                        }
                        catch (Exception e)
                        {
                            errors.incrementAndGet();
                            e.printStackTrace();
                        }
                    }
                });
            }
            latch.await();
            Assert.assertTrue(errors.get() == 0);
            System.err.println(String.format("Batch %s of %s", batch + 1, batchCount));
        }
    }

    @Test
    public void testConditionalAppendAndPoll() throws ExecutionException, InterruptedException
    {
        final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
        final List<Future<Boolean>> success = new ArrayList<>();
        final AtomicLong totalOps = new AtomicLong();
        final int perThreadOps = 1 << 24;
        queue.append(-1);
        for (int i = 0 ; i < THREAD_COUNT ; i++)
        {
            final int offset = i;
            success.add(exec.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call()
                {
                    final int operations = 1 << 24;
                    for (int i = 0 ; i < operations ; i++)
                    {
                        int v = (i * THREAD_COUNT) + offset;
                        while (true)
                        {
                            BroadcastQueue.Snap<Integer> snap = queue.snap();
                            Integer tail = snap.tail();
                            if (queue.appendIfTail(tail, v))
                            {
                                Assert.assertTrue(snap.view().contains(v));
                                while (true)
                                {
                                    Integer peek = queue.peek();
                                    if (peek == tail || peek == v || peek == queue.tail())
                                        break;
                                    queue.advanceIfHead(peek);
                                }
                                break;
                            }
                            else
                            {
                                Assert.assertFalse(snap.view().contains(v));
                            }
                        }
                        long to = totalOps.incrementAndGet();
                        if ((to & ((1 << 20) - 1)) == 0)
                            System.err.println(String.format("Completed %sM ops of %sM", to >> 20, (perThreadOps * THREAD_COUNT) >> 20));
                    }
                    return Boolean.TRUE;
                }
            }));
        }

        for (Future<Boolean> result : success)
            Assert.assertTrue(result.get());
    }

    @Test
    public void testNonCompetingDrainToWithTooManyThreads() throws InterruptedException
    {
        testNonCompetingDrainTo(true);
    }

    @Test
    public void testNonCompetingDrainToWithDedicatedThreads() throws InterruptedException
    {
        testNonCompetingDrainTo(false);
    }

    public void testNonCompetingDrainTo(boolean manyThreads) throws InterruptedException
    {
        final int batchSize = 1 << 24;
        final int batchCount = 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = manyThreads ? Runtime.getRuntime().availableProcessors() * 2 : Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            final CountDownLatch latch = new CountDownLatch(rc * 2);
            for (int i = 0 ; i < rc ; i++)
            {
                final AtomicBoolean done = new AtomicBoolean(false);
                final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        Random rand = ThreadLocalRandom.current();
                        for (int i = 0 ; i < batchSize ; i++)
                        {
                            // don't race too far ahead
                            if ((i & 1023) == 0)
                            {
                                int targetHeadroom = rand.nextInt(100000);
                                while (true)
                                {
                                    Integer head = queue.peek();
                                    if (head == null || i - head >= targetHeadroom || batchSize - head <= targetHeadroom)
                                        break;
                                }
                            }
                            queue.append(i);
                        }
                        done.set(true);
                        latch.countDown();
                    }
                });
                exec.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        Random random = ThreadLocalRandom.current();
                        List<Integer> sink = new ArrayList<>();
                        int last = -1;
                        while (!done.get() || !queue.isEmpty())
                        {
                            int fetch = (int) Math.sqrt(random.nextInt(Integer.MAX_VALUE));
                            int fetched = queue.drainTo(sink, fetch);
                            if (fetched > fetch)
                            {
                                errors.incrementAndGet();
                                System.out.println("Fetched too many items");
                            }
                            if (fetched != sink.size())
                            {
                                errors.incrementAndGet();
                                System.out.println("Fetched different number of items to returned");
                            }
                            if (sink.isEmpty())
                                continue;
                            for (int i : sink)
                            {
                                if (i != last + 1)
                                {
                                    errors.incrementAndGet();
                                    System.out.println("Incorrect next item");
                                }
                                last = i;
                            }
                            sink.clear();
                        }
                        if (last != batchSize - 1)
                        {
                            errors.incrementAndGet();
                            System.out.println("Incorrect last item (" + last + ")");
                        }
                        latch.countDown();
                    }
                });
            }
            latch.await();
            Assert.assertTrue(errors.get() == 0);
            System.err.println(String.format("Batch %s of %s", batch + 1, batchCount));
        }
    }

    @Test
    public void testCompetingDrainToWithTooManyThreads() throws InterruptedException, ExecutionException
    {
        testCompetingDrainTo(true);
    }

    @Test
    public void testCompetingDrainToWithDedicatedThreads() throws InterruptedException, ExecutionException
    {
        testCompetingDrainTo(false);
    }

    public void testCompetingDrainTo(boolean manyThreads) throws InterruptedException, ExecutionException
    {
        final int batchSize = 1 << 24;
        final int batchCount = manyThreads ? 5 : 20;
        final AtomicInteger errors = new AtomicInteger(0);
        for (int batch = 0 ; batch < batchCount ; batch++)
        {
            int rc = manyThreads ? Runtime.getRuntime().availableProcessors() * 2 : Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
            final BroadcastQueue<Integer> queue = new BroadcastQueue<>();
            final CountDownLatch latch = new CountDownLatch(rc + 1);
            final AtomicBoolean done = new AtomicBoolean(false);
            final List<Future<BitSet>> seen = new ArrayList<>();
            exec.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    Random rand = ThreadLocalRandom.current();
                    for (int i = 0 ; i < batchSize ; i++)
                    {
                        // don't race too far ahead
                        if ((i & 1023) == 0)
                        {
                            int targetHeadroom = rand.nextInt(1000000);
                            while (true)
                            {
                                Integer head = queue.peek();
                                if (head == null || i - head >= targetHeadroom || batchSize - head <= targetHeadroom)
                                    break;
                            }
                        }
                        queue.append(i);
                    }
                    done.set(true);
                    latch.countDown();
                }
            });
            for (int i = 0 ; i < rc ; i++)
            {
                seen.add(exec.submit(new Callable<BitSet>()
                {
                    @Override
                    public BitSet call()
                    {
                        final BitSet seen = new BitSet(batchSize);
                        Random random = ThreadLocalRandom.current();
                        List<Integer> sink = new ArrayList<>();
                        while (!done.get() || !queue.isEmpty())
                        {
                            int fetch = (int) Math.sqrt(random.nextInt(Integer.MAX_VALUE));
                            int fetched = queue.drainTo(sink, fetch);
                            if (fetched > fetch)
                            {
                                errors.incrementAndGet();
                                System.out.println("Fetched too many items");
                            }
                            if (fetched != sink.size())
                            {
                                errors.incrementAndGet();
                                System.out.println("Fetched different number of items to returned");
                            }
                            if (sink.isEmpty())
                                continue;
                            for (int i : sink)
                            {
                                if (seen.get(i))
                                {
                                    errors.incrementAndGet();
                                    System.out.println("Seen same number repeatedly on same thread");
                                }
                                seen.set(i);
                            }
                            sink.clear();
                        }
                        latch.countDown();
                        return seen;
                    }
                }));
            }
            latch.await();
            BitSet merge = new BitSet(batchSize);
            for (Future<BitSet> f : seen)
            {
                BitSet one = f.get();
                for (int i = 0 ; i < batchSize ; i++)
                {
                    if (one.get(i))
                    {
                        if (merge.get(i))
                        {
                            errors.incrementAndGet();
                            System.out.println("Seen same number repeatedly on different threads");
                        }
                        merge.set(i);
                    }
                }
            }
            for (int i = 0 ; i < batchSize ; i++)
            {
                if (!merge.get(i))
                {
                    errors.incrementAndGet();
                    System.out.println("Missing number");
                }
            }
            Assert.assertTrue(errors.get() == 0);
            System.err.println(String.format("Batch %s of %s", batch + 1, batchCount));
        }
    }

}
