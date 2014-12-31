package bes.concurrent.order;

/**
 * see {@link Order#startMulti}
 */
public final class MultiOp implements AutoCloseable
{
    private final Order order;
    private Op current;

    public MultiOp(Order order)
    {
        this.order = order;
    }

    /**
     * Called periodically to indicate we have reached a safe point wrt data guarded by this OpOrdering
     */
    public void restart()
    {
        // only swap the operation if we're behind the present
        if (current == null || current.phase != order.current)
        {
            if (current != null)
                current.close();
            current = order.start();
        }
    }

    public Op current()
    {
        return current;
    }

    /**
     * Called once our transactions have completed. May safely be called multiple times, with each extra call
     * a no-op.
     */
    public void close()
    {
        if (current != null)
            current.close();
        current = null;
    }
}

