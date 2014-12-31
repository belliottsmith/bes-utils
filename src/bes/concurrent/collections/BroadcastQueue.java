package bes.concurrent.collections;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A simple queue API on which all operations are non-blocking and atomic wrt each other (except Iterator removal).
 * This queue is designed with predictable performance and ease of understanding in mind, NOT absolute performance,
 * though it should perform very well still.
 *
 * There are a number of differences to {@link java.util.concurrent.ConcurrentLinkedQueue}:
 * (see below for expansion)
 * <p>1) All methods are lock-free
 * <p>2) CAS-like operations are supported for modifying the head and tail of the queue
 * <p>3) CAS-like removes are supported through AtomicIterator
 * <p>4) fast but unsafe (not guaranteed to persist) removes are supported through normal Iterator
 * <p>5) the queue may be cheaply snapshot'd, to provide consistent repeat iterations
 * <p>6) multiple consumers of the queue are able to see (view) the same stream of events, through a NonBlockingQueueView,
 * which may also optionally co-ordinate removal of items from the shared stream.
 *
 * To support all of this functionality, there are two distinct semantics this queue supports, simultaneously, that
 * should be understood; namely: advance, and remove. Either of these taken against just one NBQ will behave much
 * like poll() on any normal queue, however once there are multiple views on the queue these two concepts diverge.
 *
 * advance() simply moves the current view of the queue forwards, acting like a poll() on that view only, leaving all
 * other queues to consume that element when they reach it.
 * remove(), on the other hand, effectively flattens all queues into one queue for purposes of that operation,
 * i.e. if all queues were to only use remove() the semantics would be the same as if all were using the same queue.
 * To put it another way, remove() operations are atomic and visible across all queues, so once removed you can be
 * certain that nobody else also removed the item, nor that anybody else will now see it after.
 * Consider that some methods with one semantic may have specific interaction characteristics when interleaved with actions
 * of the opposing semantics. However these follow naturally from this specification.
 *
 * <p>Unsafe (regular) iterator removal:
 * To help certain use cases, Iterator removal is supported from arbitrary positions, but it is not guaranteed to
 * succeed, or to persist if it does succeed. If it is interleaved with competing removes it may be ignored, and may
 * even restore previously successfully removed items. It will never remove the tail item. It should not be used in
 * situations where removal is anything more than a convenience, or where consistency of removal is required, however
 * it is fast and in aggressive testing achieves correct removal >99% of the time.
 *
 * <p>Safe iterator removal:
 * A stronger remove() is provided for cases where safe removal is necessary, for which the AtomicIterator interface
 * exposes the functionality. In this case if you call atomicRemove() and true is returned, you can be certain that
 * the item has been removed, will not reappear, and that no other thread has also independently removed it; if false
 * is returned the item has already been deleted by another process. i.e. it is atomic like all the other operations on
 * the queue. We do not force this stronger remove() for all operations because it is appreciably more expensive.
 * Note that references to the data may or may not be freed up immediately.
 *
 * <p>Snapshots and Views:
 * A view is a mostly read-only copy of the queue from the position of its 'parent' when created, and including all items
 * added from them on, supporting all of the same methods as this queue for polling, iterating or removing items.
 * See {@link BroadcastView}.
 * A snapshot is like a view, but also captures the end at the time of creation, so that no new items are returned.
 * It only supports iteration, and conversion back into a View,
 *
 * Note that the queue will always keep a reference to the last item polled, preventing GC on this item.
 *
 * @param <V>
 */
public class BroadcastQueue<V> extends BroadcastView<V> implements Iterable<V>
{

    /**
     * The queue is effectively two pointers into a singly linked list which can only safely be appended to;
     * advance() only advances the head pointer, with the prefix left for GC to deal with. As a result we can easily
     * create persistent views and snapshots of the list, which are supported through the snap() and view() methods
     * respectively. Calls to advance?() on a view of the queue do not affect any other view, including the original.
     * Calls to remove(), however, are reflected in this queue and all views on it, and are atomic with respect to all
     * other remove() operations on all other views and this queue.
     */

    /**
     * the tail pointer of the list - this is kept mostly exactly up-to-date (except brief races which cause it to lag)
     * since the list is immutably traversable to the tail, any chain of next pointers will bring you to the real tail
     */
    private volatile Node<V> tail;

    static final AtomicReferenceFieldUpdater<BroadcastQueue, Node> tailUpdater = AtomicReferenceFieldUpdater.newUpdater(BroadcastQueue.class, Node.class, "tail");

    public BroadcastQueue()
    {
        super(new Node<V>(null));
        tail = head;
    }

    /**
     * Add the provided item to the end of the queue
     *
     * @param append
     */
    public void append(V append)
    {
        Node<V> newTail = new Node<>(append);
        while (true)
        {
            Node<V> tail = tailNode();
            tail.initSuccessor(newTail);
            if (Node.nextUpdater.compareAndSet(tail, null, newTail))
                return;
        }
    }

    /**
     * Add <code>append</code> to the end of the queue iff the end of the queue is currently
     * <code>expectedTail</code>. Note that this works even if the queue is now empty but the last item
     * prior to the queue being empty was <code>expectedTail</code>.
     *
     * @param expectedTail the last item we expect to be in the queue, or the last item returned if empty
     * @param append the item to add
     * @return true iff success
     */
    public boolean appendIfTail(V expectedTail, V append)
    {
        Node<V> tail = tailNode();
        if (expectedTail != tail.value)
            return false;
        Node<V> newTail = new Node<>(append);
        tail.initSuccessor(newTail);
        return Node.nextUpdater.compareAndSet(tail, null, newTail);
    }

    /**
     * Advances the queue past the current tail, leaves GC to deal with the mess.
     */
    public void clear()
    {
        head = tailNode();
    }

    /**
     * Returns the tail of the queue. NOTE that this succeeds even if the queue is empty,
     * returning the most recently polled item in this case. If the queue has always been empty, null
     * is returned.
     * @return the tail of the queue
     */
    public V tail()
    {
        return tailNode().value;
    }

    /**
     * Finds the tail and updates the global tail property if the tail we find is not the same
     *
     * @return
     */
    protected final Node<V> tailNode()
    {
        while (true)
        {
            Node<V> prev = tail;
            Node<V> tail = prev;
            while (tail.next != null)
                tail = tail.next;
            // we perform a cas to make sure we never get too far behind the head pointer,
            // to avoid retaining more garbage than necessary
            if (prev == tail || tailUpdater.compareAndSet(this, prev, tail))
                return tail;
        }
    }

    /**
     * advance the queue by at most maxItems, placing any items advanced onto the provided list.
     * any remove() operations performed during this operation may be ignored, and the items still
     * appear on the queue, however it is atomic wrt all advance() operations on this queue.
     * If the queue can be guaranteed to be smaller than the requested maxItems, this operation is
     * extremely fast, however otherwise it falls back to {@link BroadcastView#drainTo}
     *
     * @param list the list to append items to
     * @return the number of items appended to the list
     */
    public int drainTo(List<? super V> list, int maxItems)
    {
        Node<V> cursor, tail;
        while (true)
        {
            cursor = this.head;
            tail = tailNode();
            long maxLength = cursor.delta(tail);
            assert maxLength >= 0;
            if (maxLength > maxItems)
                return super.drainTo(list, maxItems);
            if (headUpdater.compareAndSet(this, cursor, tail))
                break;
        }
        int items = 0;
        cursor = cursor.next;
        while (cursor != null && !cursor.isAfter(tail))
        {
            V next = cursor.value;
            if (!cursor.isDeleted())
            {
                list.add(next);
                items++;
            }
            cursor = cursor.next;
        }
        return items;
    }

    /**
     * @return the size of the queue, overcounted by any items that have been removed from its middle
     */
    public int approxSize()
    {
        Node<V> head = this.head, tail = tailNode();
        long maxLength = head.delta(tail);
        if (maxLength > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        assert maxLength >= 0;
        return (int) maxLength;
    }

    /**
     * Returns an Iterable over all items currently in the queue, excluding some subset of those that are deleted
     * using Iterator.remove().
     *
     * @return
     */
    public Snap<V> snap()
    {
        Node<V> head = this.head;
        return new SnapImpl(head, tailNode());
    }

    /**
     * Represents a consistent sub-queue, whose start and end are unaffected by poll() and append() operations on the
     * queue it is formed from, but will reflect any 'successful' Iterator removals, as defined elsewhere.
     * @param <V>
     */
    public static interface Snap<V> extends Iterable<V>
    {
        /**
         * @return the last item in this snap, or null if the snap is empty
         */
        public V tail();

        /**
         * @return a view of the entire queue, starting at the same position as this Snap
         */
        public BroadcastView<V> view();

        public bes.concurrent.collections.AtomicIterator<V> iterator();

        /** Extend the tail of this snap to the end of the queue it was constructed from, as it now stands */
        public Snap<V> extend();
    }

    private class SnapImpl implements Snap<V>
    {
        final Node<V> head;
        final Node<V> tail;
        public SnapImpl(Node<V> head, Node<V> tail)
        {
            this.head = head;
            this.tail = tail;
        }

        @Override
        public bes.concurrent.collections.AtomicIterator<V> iterator()
        {
            return new SliceIterator(head, tail);
        }

        public Snap<V> extend()
        {
            return new SnapImpl(head, tailNode());
        }

        @Override
        public V tail()
        {
            return tail == head ? null : tail.value;
        }

        public BroadcastView<V> view()
        {
            return new BroadcastView<>(head);
        }

    }

}
