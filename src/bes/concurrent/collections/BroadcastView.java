package bes.concurrent.collections;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * See {@link BroadcastQueue}
 *
 * A mostly-readonly view on a {@link BroadcastQueue}. Advancing this view will not affect the position of the queue
 * it is a view of, or other views on the queue, however successful remove operations will be reflected in the original
 * queue, and all other views. atomicRemove() and BV.remove() are atomic operation across all views, i.e. only one such
 * operation may return true out of any identical operation on any view on the queue (including the original).
 *
 * This is kept as a distinct class from BroadcastQueue to ensure that only one appendable copy of any queue exists.
 * Whilst this isn't strictly necessary for function, it makes for easier reasoning about behaviour and prevents
 * accidentally suboptimal behaviour (e.g. with tail pointers)
 */
public class BroadcastView<V> implements Iterable<V>
{

    static final class Node<V>
    {

        static final long DELETED = 1;
        static final AtomicLongFieldUpdater<Node> stateUpdater = AtomicLongFieldUpdater.newUpdater(Node.class, "state");
        static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

        /**
         * encodes both the safe (atomic) deleted status of the item, and its order in the queue; the order is used to
         * make implementing snapshots easy, so that it will safely iterate only the items present when it was created.
         * this also means keeping less garbage around than the prior approach of fixing any snapshot tail in the
         * list indefinitely.
         *
         * safe removes work by atomically swapping the deleted bit to true, and returning the success
         * of the operation. before returning an unsafe remove is attempted to maybe free up the node immediately,
         * but success is irrelevant. any read operations that encounter a node with its deleted bit set will attempt
         * to unsafe delete it also, and then immediately discard it and continue to the next node.
         *
         * the lowest bit is the deleted status, so that it does not affect order comparisons
         */
        // no need to be volatile universally; only delete/isDeleted require it, but requires unsafe
        private volatile long state;

        volatile Node<V> next;
        final V value;

        public Node(V value)
        {
            this.value = value;
        }

        public boolean isDeleted()
        {
            return (state & DELETED) != 0;
        }

        boolean delete()
        {
            long current = state;
            return ((current & DELETED) == 0) && stateUpdater.compareAndSet(this, current, current | DELETED);
        }

        // setup the status (order) of the node that will (may) directly proceed this one. Multiple nodes may have
        // this called on them, but only one will be atomically added as the successor; all others will have their
        // state re-initialised by another node
        void initSuccessor(Node<?> successor)
        {
            successor.state = (state & ~DELETED) + 2;
        }

        boolean isAfter(Node<V> that)
        {
            return this.state > that.state;
        }

        long delta(Node<V> that)
        {
            return (that.state - this.state) / 2;
        }

    }

    // the head of the queue; this is a "dummy" head that moves as the view is consumed, i.e. it always points to
    // the previous head of the queue, with its next pointer the current head
    volatile Node<V> head;

    protected static final AtomicReferenceFieldUpdater<BroadcastView, Node> headUpdater = AtomicReferenceFieldUpdater.newUpdater(BroadcastView.class, Node.class, "head");

    protected BroadcastView(Node<V> head)
    {
        this.head = head;
    }

    /**
     * advances the head of the queue iff it is currently the the provided node
     * @param expectedHead the item we expect to be the current head
     * @return true iff success
     */
    protected final boolean advanceIfHead(Node expectedHead)
    {
        assert expectedHead != null;
        Node head = this.head;
        Node next = head.next;
        return next == expectedHead && headUpdater.compareAndSet(this, head, next);
    }

    /**
     * advances the current view iff the provided item is currently the head of the queue, by object identity (==)
     *
     * @param expectedHead the item we expect to be first in the queue
     * @return true iff success
     */
    public boolean advanceIfHead(V expectedHead)
    {
        assert expectedHead != null;
        Node<V> next = head.next;
        if (next != null && next.value == expectedHead)
            return advanceIfHead(next);
        return false;
    }

    /**
     * advances the head of the queue iff it is currently the the provided node
     * @param expectedHead the item we expect to be the current head
     * @return true iff success
     */
    protected final boolean removeIfHead(Node expectedHead)
    {
        assert expectedHead != null;
        Node head = this.head;
        Node next = head.next;
        return next == expectedHead && head.delete() && headUpdater.compareAndSet(this, head, next);
    }

    /**
     * advances the current view iff the provided item is currently the head of the queue, by object identity (==)
     *
     * @param expectedHead the item we expect to be first in the queue
     * @return true iff success
     */
    public boolean removeIfHead(V expectedHead)
    {
        assert expectedHead != null;
        Node<V> next = head.next;
        if (next != null && next.value == expectedHead)
            return removeIfHead(next);
        return false;
    }

    /**
     * advance() the queue at most maxItems times, placing any item yielded onto the provided list.
     * if at any point the queue is exhausted before maxItems have been visited, return the number
     * of items added to the list.
     *
     * this method is ordinarily more efficient than simply successively calling advance() and add(),
     * although if there is a lot of competition will fallback to this approach.
     *
     * @param list the list to append items to
     * @param maxItems maximum number of items to append to the list
     * @return the number of items appended to the list
     */
    public int drainTo(List<? super V> list, int maxItems)
    {
        int curItems = 0;
        // drain in batches of at most maxItems, but maybe fewer; if at any point we fail to grab at least
        // half of the items we batched, fall-back to individual advance() operations
        fast: while (curItems < maxItems)
        {
            // first find a chain of length <= maxItems
            Node<V> head = this.head, tail = head;
            int chainLength = 0;
            while (chainLength < maxItems)
            {
                Node<V> next = tail.next;
                if (next == null)
                    break;
                tail = next;
                chainLength++;
            }

            if (chainLength == 0)
                return curItems;

            // then try to atomically advance the entire chain at once - if we fail, walk forwards until we
            // hit the end of the chain or we successfully advance the remaining portion
            int skipped = 0;
            while (!headUpdater.compareAndSet(this, head, tail))
            {
                head = head.next;
                // if we reach an empty chain, we abort and try the slow route
                // we test !tail.isAfter(head) because we want to exit if tail == head (becuase this would be
                // a wasteful cas), so we want the test to be true regardless of state changes due to a delete;
                // in this scenario we would be testing !(x > x + 1), which is of course true.
                if (head == null || !tail.isAfter(head))
                    break fast;
                skipped++;
            }

            // finally we walk the remainder of the chain (skipping the dummy head pointer), and add to the list
            // any nodes that aren't deleted
            int claimed = 0;
            head = head.next;
            // in this case we don't care if we miss the last item due to races with delete, as we would discount it anyway
            // but we want to see the tail item, so we want it to be true when they are equal.
            while (head != null && !head.isAfter(tail))
            {
                if (!head.isDeleted())
                {
                    claimed++;
                    list.add(head.value);
                }
                head = head.next;
            }

            curItems += claimed;

            // if we skipped more nodes than we ended up adding, fall-back to slow route. this may be
            // overly pessimistic if there are a lot of stale deleted nodes floating about, but this
            // should be rare
            if (skipped > claimed)
                break;
        }

        while (curItems < maxItems)
        {
            V next = advance();
            if (next == null)
                return curItems;
            list.add(next);
            curItems++;
        }
        return curItems;
    }

    public boolean contains(Object o)
    {
        for (Object c : this)
            if (o.equals(c))
                return true;
        return false;
    }

    /**
     * @return true iff the queue is currently empty
     */
    public final boolean isEmpty()
    {
        return head.next == null;
    }

    /**
     * This method is expensive, O(n) on size of queue.
     * @return all items inserted into the queue prior to the method returning that had not been advanced()
     * past prior to the method entry
     */
    public int size()
    {
        Node<V> cur = head.next;
        int size = 0;
        while (cur != null)
        {
            size++;
            cur = cur.next;
        }
        return size;
    }

    /**
     * @return the first Node in the queue, after atomically advancing the head past it
     */
    protected final Node<V> advanceNode()
    {
        while (true)
        {
            // if we have a non-null next, cas the head to it, if not fail
            Node<V> head = this.head;
            Node<V> next = head.next;
            if (next == null)
                return null;
            if (headUpdater.compareAndSet(this, head, next))
                return next;
        }
    }

    /**
     * @return the previous head of the queue, after atomically advancing the queue
     */
    public V advance()
    {
        while (true)
        {
            Node<V> poll = advanceNode();
            if (poll == null)
                return null;
            if (!poll.isDeleted())
                return poll.value;
        }
    }

    /**
     * @return the previous head of the queue, after atomically *removing* it. Note that this
     * method will be visible to all views and snaps of the queue.
     */
    public V remove()
    {
        while (true)
        {
            Node<V> poll = advanceNode();
            if (poll == null)
                return null;
            // TODO: should track pred and call unsafeRemove(), to keep other views tidy
            if (poll.delete())
                return poll.value;
        }
    }

    /**
     * Looks through all items remaining in this view for the provided item, and if found atomically removes
     * it from this queue and all other views/snaps of the queue
     * @param item the item to remove
     * @return true if the item was found and atomically removed, false otherwise
     */
    public boolean remove(V item)
    {
        bes.concurrent.collections.AtomicIterator<V> iter = iterator();
        while (iter.hasNext())
        {
            if (item.equals(iter.next()) && iter.atomicRemove())
                return true;
        }
        return false;
    }

    /**
     * @return The next item to be returned from the queue, or null of none.
     */
    public V peek()
    {
        while (true)
        {
            Node<V> peek = head.next;
            if (peek == null)
                return null;
            if (!peek.isDeleted())
                return peek.value;
            advanceIfHead(peek);
        }
    }

    /**
     * @return A view over all items inserted or advanced after it was created. Equivalent to view().iterator()
     */
    public bes.concurrent.collections.AtomicIterator<V> iterator()
    {
        return new SliceIterator(head, null);
    }

    /**
     * @return an Iterable over all items from the current head, excluding those that are removed
     * of those that are deleted through Iterator.remove().
     * Whilst this object is reachable, all items polled here, but not in the view, after it was created will
     * continue to also be retained.
     */
    public BroadcastView<V> view()
    {
        Node<V> head = this.head;
        return new BroadcastView<>(head);
    }

    /**
     * Makes a very poor (but simple and quick) effort to remove the item from the queue; the precondition is that
     * pred was at some point the direct predecessor of remove.
     * This method works on the assumption that the queue is only ever appended to, and that the only modifications to the
     * pointers in the middle of the queue are these poor delete attempts; given this second requirement and that individual
     * deletes always leave non-deleted items reachable, when deletes race we can only lose deletes, by restoring a pointer
     * to an already deleted node. Non-deleted items will always be reachable.
     *
     * @param pred once a direct predecessor of remove
     * @param remove the node to remove
     */
    void unsafeRemove(Node<V> pred, Node<V> remove, Node<V> next)
    {
        if (next == null)
            return;
        pred.next = next;
        if (head == remove)
            advanceIfHead(remove);
    }

    /**
     * An iterator over a NBQV, that supports an arbitrary end point or iteration to the end of the list
     * (which will continue to be updated as the list is appended to, even after the iterator is exhausted)
     */
    class SliceIterator implements bes.concurrent.collections.AtomicIterator<V>
    {

        Node<V> pred, cur, next;
        final Node<V> end;

        public SliceIterator(Node<V> head, Node<V> end)
        {
            this.end = end;
            this.cur = head;
            this.next = head.next;
            hasNext();
        }

        /**
         * @return true if a call to next() is possible
         */
        @Override
        public boolean hasNext()
        {
            while (true)
            {
                if (next == null)
                    return false;

                // races to check state on same node are irrelevant due to checking isDeleted() after
                if (end != null && next.isAfter(end))
                    return false;
                if (!next.isDeleted())
                    return true;

                // if it's deleted, unsafe remove it and move to the next item
                Node<V> next2 = next.next;
                BroadcastView.this.unsafeRemove(cur, next, next2);
                next = next2;
            }
        }

        /**
         * @return the next item in the queue. the behaviour is undefined if next() is called past the natural
         * end of the iterator, so should ordinarily be guarded by a call to hasNext()
         */
        @Override
        public V next()
        {
            // shift positions along one
            pred = cur;
            cur = next;
            next = next.next;
            // call hasNext() to ensure the node we have moved to is valid for this iterator.
            // we do this in case the user doesn't; although that would be undefined behaviour in general there may be
            // some cases where the user somehow knows there will be more items, so does not call hasNext()
            hasNext();
            return cur.value;
        }

        /**
         * maybe remove the previously returned item from the queue. if successful may be reflected in other views,
         * however may spuriously be undone by other remove() operations. In general there is a high likelihood of success;
         * ordinarily at least 99%. but it should not be used were consistency or atomicity is a requirement.
         */
        @Override
        public void remove()
        {
            if (pred == null)
                throw new NoSuchElementException();
            BroadcastView.this.unsafeRemove(pred, cur, next);
            cur = pred;
            pred = null;
        }

        /**
         * atomically remove the previously returned item from the queue and all other queues
         * @return true iff this operation removed the item from the queue; false if it was removed by another call
         */
        public boolean atomicRemove()
        {
            if (pred == null)
                throw new NoSuchElementException();
            if (cur.delete())
            {
                remove();
                return true;
            }
            return false;
        }
    }
}
