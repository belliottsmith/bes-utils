package bes.concurrent.collections;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A simple wrapper around AbstractSimpleCollection, offering lock-free append in constant time,
 * wait-free removal in constant time, emptiness checking in expected constant time, and
 * in-order traversal and size calculation in linear time.
 *
 * @param <V>
 */
public final class SimpleCollection<V> extends AbstractSimpleCollection<SimpleCollection<V>.Item> implements Iterable<V>
{

    private final Function<Item, V> valueProjection;
    public SimpleCollection()
    {
        this((i) -> i.item);
    }
    public SimpleCollection(Function<Item, V> valueProjection)
    {
        this.valueProjection = valueProjection;
    }

    /**
     * Add the provided to the list; this item will be visible in the list's iterator, in insertion order, until
     * the delete() method on the returned handle is called, or the iterator's remove() method is called, at which
     * point both the handle's item() method will return null and future iterations of the list will omit the item.
     *
     * @param item item to insert
     * @return handle for item in list, permitting efficient removal
     */
    public Item add(V item)
    {
        if (item == null)
            throw new NullPointerException();
        Item insert = new Item(item);
        add(insert);
        return insert;
    }

    /**
     * Same as {@link #add}, but caller must guarantee no concurrent add() or addExclusive() overlap with this call
     * @param item item to insert
     * @return handle for item in list, permitting efficient removal
     */
    public Item addExclusive(V item)
    {
        if (item == null)
            throw new NullPointerException();
        Item insert = new Item(item);
        addExclusive(insert);
        return insert;
    }

    public Result visit(Function<V, Action> f)
    {
        return _visit((i) -> {
            V value = i.item;
            if (value == null)
                return Action.CONTINUE;
            return f.apply(value);
        });
    }

    public void forEach(Consumer<? super V> action)
    {
        visit((v) -> { action.accept(v); return Action.CONTINUE; });
    }

    public boolean isEmpty()
    {
        return _visit(NOT_EMPTY) == Result.SUCCESS;
    }

    public int size()
    {
        Count count = new Count();
        _visit(count);
        return count.count;
    }

    public Iterator<V> iterator()
    {
        return super.iterator(valueProjection);
    }

    public final class Item extends AbstractSimpleCollection.Node
    {
        volatile V item;

        public Item(V item)
        {
            this.item = item;
        }

        public V item()
        {
            return item;
        }

        public boolean isDeleted()
        {
            return item == null;
        }

        public Node delete()
        {
            item = null;
            return remove();
        }
    }

    private static final class Count implements Function<SimpleCollection<?>.Item, Action>
    {
        int count;
        public Action apply(SimpleCollection<?>.Item item)
        {
            if (!item.isDeleted())
            {
                count++;
                return Action.CONTINUE;
            }
            return Action.REMOVE;
        }
    }

    private static final Function<SimpleCollection<?>.Item, Action> NOT_EMPTY =
    (i) -> i.item == null ? Action.CONTINUE : Action.SUCCEED;
}
