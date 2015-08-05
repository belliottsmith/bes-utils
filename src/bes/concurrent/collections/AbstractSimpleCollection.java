package bes.concurrent.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

/**
 * A simple abstract concurrent collection for building more complex data structures.
 * Essentially supports three operations: append and delete in expected constant time, and iteration in linear time.
 *
 * This is a (non-circular) doubly-linked-list on which all links are maintained with weak promises.
 * The prev and next pointers each, if followed transitively, will eventually reach the true prev/next node,
 * defined as the following non-deleted node. The head and tail are similarly defined as preceding
 * (by some chain of next pointers) the true head/tail.
 *
 * If a list has no deletions, or only non-competing deletes, its prev/next pointers will be perfect.
 * In the face of multiple adjacent deletes with overlapping execution we only guarantee that we do
 * not remove any nodes that haven't been deleted from the list. We may reintroduce previously deleted
 * nodes, however we _do our best_ to find the longest string of deleted neighbours directly adjacent
 * to the node we are removing, and remove those along with us. Even with many competing deletes over
 * a long period of time we will still tend steadily with this approach to the elimination of all deleted
 * nodes (and generally quite rapidly).
 *
 * This can be demonstrated safe quite straight-forwardly: We limit ourselves to only modifying the reverse pointer
 * of the nearest non-deleted node (i.e. the prev pointer of the node reached by next links, and vice versa),
 * OR a deleted node nearer than it (if maxRemoveLength is small, or the node is itself deleted as we are performing the edit).
 * We also only ever assign it a value that has the same constraint (the opposite live node, or a nearer deleted node).
 * We never insert in the middle of the list, deletion is permanent, and we never remove the tail. As such, interleavings
 * can at most corrupt a deleted portion of the list, potentially reintroducing nodes, or resulting in next/prev links
 * reflecting slightly different lists, but neither of these break our constraints.
 *
 * TODO: use cas when detect competition
 */
public class AbstractSimpleCollection<N extends AbstractSimpleCollection.Node>
{

    // dummy head pointer. tail pointer is only useful for insertion.
    // both are only <= their true position, which can be reached
    // from any node (deleted or not) prior to them via a chain
    // of next links.
    protected volatile Node head, tail;

    public AbstractSimpleCollection()
    {
        head = tail = new Node() {
            protected boolean isDeleted() { return true; }
            protected Node delete() { return null; }
        };
    }

    /**
     * @param insert the node to append to the list; this node should never have
     *               been inserted into any list
     */
    protected void add(N insert)
    {
        assert insert.next == null && insert.prev == null;

        Node tail = this.tail;
        while (true)
        {
            Node next = tail.next;
            if (next == null)
            {
                insert.prev = tail;
                if (!nextUpdater.compareAndSet(tail, null, insert))
                    continue;
                tailUpdater.compareAndSet(this, tail, insert);
                return;
            }
            tail = next;
        }
    }

    /**
     * @param insert the node to append to the list with exclusive access (i.e. caller guarantees
     *               (no concurrent add operations by some external method).
     *               the node should never have been inserted into any list
     */
    protected void addExclusive(N insert)
    {
        assert insert.next == null && insert.prev == null;

        Node tail = this.tail;
        assert tail.next == null;
        insert.prev = tail;
        nextUpdater.lazySet(tail, insert);
        this.tail = insert;
    }

    /**
     * Controls behaviour of in-order visitation of visit() method;
     * FAIL     terminates the method returning Result.FAILURE;
     * SUCCEED  terminates the method returning Result.SUCCESS;
     * CONTINUE indicates the node just visited is still alive,
     *          and the remainder of the list is to be visited
     * REMOVE   indicates the node just visited is (now) deleted
     */
    public static enum Action
    {
        FAIL(Result.FAILURE),
        SUCCEED(Result.SUCCESS),
        CONTINUE(null),
        REMOVE(null);

        final Result result;
        Action(Result result)
        {
            this.result = result;
        }
    }

    /**
     * Returns result of in-order visitation;
     * SUCCESS/FAILURE indicates corresponding Action was returned by visitation function;
     * EXHAUSTED indicates the entire list was visited without either being returned
     */
    public static enum Result
    {
        FAILURE, SUCCESS, EXHAUSTED
    }

    /**
     * Visits each node in sequence, _including deleted nodes_, applying the provided function.
     * If the node is deleted, REMOVE should ideally be returned although this is not required.
     * Behaviour is defined by {@link Action}
     *
     * @param f function to apply to each node; return value determines visitation behaviour
     * @return Result of visit function
     */
    protected Result _visit(Function<? super N, Action> f)
    {
        // rp: real == nearest predecessor that was live when visited, or head if none
        // ep: effective == nearest predecessor that was live when visited, or null if none
        // dp: direct == the immediate predecessor, live or otherwise
        Node rp = head, ep = null, dp = rp;
        N next = (N) rp.next;

        while (true)
        {
            if (next == null)
                return Result.EXHAUSTED;

            Action action = f.apply(next);
            switch (action)
            {
                case SUCCEED:
                case FAIL:
                    if (dp != ep || dp != next.prev)
                        repair(rp, ep, dp, next);
                    return action.result;
                case REMOVE:
                    assert next.isDeleted();
                    // prev assignments will be covered by final memory barrier in repair() call
                    next.prev = ep;
                    break;
                case CONTINUE:
                    if (dp != ep || dp != next.prev)
                        repair(rp, ep, dp, next);
                    rp = ep = next;
            }

            dp = next;
            next = (N) next.next;
        }
    }

    /**
     * Returns an Iterator over the contents of this list, transformed by the provided function.
     * The function should return a non-null value for any non-deleted node, else the list may
     * end up in an inconsistent state
     *
     * @param f transformation function
     * @return an iterator over the contents of this list
     */
    protected <V> Iterator<V> iterator(final Function<? super N, ? extends V> f)
    {
        return new Iterator<V>()
        {
            V nextv;
            Node rp = head, ep = null, dp = rp;

            public boolean hasNext()
            {
                if (nextv != null)
                    return true;

                N next = (N) rp.next;
                while (true)
                {
                    if (next == null)
                        return false;

                    V v = f.apply(next);
                    if (v != null)
                    {
                        if (dp != ep)
                            repair(rp, ep, dp, next);
                        rp = ep = dp = next;
                        nextv = v;
                        return true;
                    }

                    assert next.isDeleted();
                    // prev assignments will be covered by final memory barrier in repair() call
                    next.prev = ep;
                    dp = next;
                    next = (N) next.next;
                }
            }

            public V next()
            {
                if (nextv == null && !hasNext())
                    throw new NoSuchElementException();
                V r = nextv;
                nextv = null;
                return (V) r;
            }

            public void remove()
            {
                ep = dp.delete();
                dp = rp = ep == null ? head : ep;
            }
        };
    }

    /**
     * Repair the list, rewriting prev, next and head pointers as necessary to edit out
     * previously deleted elements
     *
     * @param rp real == nearest predecessor that was live when visited, or head if none
     * @param ep effective == nearest predecessor that was live when visited, or null if none
     * @param dp direct == the immediate predecessor, live or otherwise
     * @param next
     */
    private void repair(Node rp, Node ep, Node dp, Node next)
    {
        // edit out a chain of deleted items
        next.prev = ep;
        if (ep == null)
        {
            nextUpdater.lazySet(rp, next);
            headUpdater.compareAndSet(this, rp, dp);
        }
        else
        {
            rp.next = next;
        }
    }

    public abstract class Node
    {
        volatile Node next;
        Node prev; // no need for volatile semantics of its own, as can be guarded by those of "next"

        /**
         * @return true if the node is deleted. Once true this value should never revert.
         */
        protected abstract boolean isDeleted();

        /**
         * If implemented, should set its state so that isDeleted() always returns true, then call remove().
         * By default, throws UnsupportedOperationException()
         *
         * @return the result of calling remove()
         */
        protected Node delete()
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Attempt to edit ourselves out of the list. isDeleted() should be true
         * before calling this method.
         *
         * @return the best (live, or maxRemoveLength) predecessor
         */
        protected Node remove()
        {
            assert isDeleted();
            // in case the list has some phantom elements, we try to remove ourselves
            // and any contiguous adjacent range of deleted nodes

            Node n = next;
            // if we are the tail of the list, we cannot be removed
            if (n == null)
                return null;

            int len = 0;
            // find our live predecessor
            Node p = prev;
            while (true)
            {
                if (p == null)
                {
                    // we have no live predecessor; cleanupHead() will remove us
                    cleanupHead();
                    return null;
                }

                if (!p.isDeleted() || ++len == 10)
                    break;

                p = p.prev;
            }

            // find our live successor (or the tail)
            Node n2;
            while (++len <= 10 && n.isDeleted() && (n2 = n.next) != null)
                n = n2;

            // try to fix up the pointers of our live neighbours
            n.prev = p;
            p.next = n;
            return p;
        }
    }

    private void cleanupHead()
    {
        Node ph = head, h = ph, n;
        while ((n = h.next) != null && n.isDeleted())
        {
            // prev assignment guarded by final head assignment
            n.prev = null;
            h = n;
        }
        if (ph != h)
            headUpdater.compareAndSet(this, ph, h);
    }

    private static final AtomicReferenceFieldUpdater<AbstractSimpleCollection.Node, AbstractSimpleCollection.Node>
    nextUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractSimpleCollection.Node.class, AbstractSimpleCollection.Node.class, "next");

    private static final AtomicReferenceFieldUpdater<AbstractSimpleCollection, AbstractSimpleCollection.Node>
    headUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractSimpleCollection.class, AbstractSimpleCollection.Node.class, "head"),
    tailUpdater = AtomicReferenceFieldUpdater.newUpdater(AbstractSimpleCollection.class, AbstractSimpleCollection.Node.class, "tail");
}
