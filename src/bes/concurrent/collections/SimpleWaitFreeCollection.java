/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package bes.concurrent.collections;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * This class builds upon SimpleCollection, furnishing each inserting thread its own such collection
 * that it modifies with exclusive access. These collections are concatenated for traversal. As such
 * traversal order is arbitrary, and the only supported operations are append, remove and traverse,
 * however all of these operations are wait-free, except the first add() for any thread, which is only
 * lock-free (unless preceded by a call to register() by the thread)
 */
public class SimpleWaitFreeCollection<V> implements Iterable<V>
{

    // all of the collections that constitute this aggregate collection
    final SimpleCollection<MarkableReference<V>> allCollections = new SimpleCollection<>();

    // a shared projection method, to avoid waste
    final Function<SimpleCollection<V>.Item, V> itemProjection = (i) -> i.item;

    // the per-thread collections for exclusive modification
    final ThreadLocal<CollectableReference<V>> threadCollection = new ThreadLocal<CollectableReference<V>>()
    {
        protected CollectableReference<V> initialValue()
        {
            // construct the collection
            SimpleCollection<V> collection = new SimpleCollection<>(itemProjection);
            // construct our threadlocal reference to it,
            // which will be collected when the thread's threadlocalmap is
            CollectableReference<V> ret = new CollectableReference<V>(collection);
            // construct our shared reference to it, which can be marked for removal
            // once it is empty and will no longer be appended to
            MarkableReference<V> mark = new MarkableReference<>(collection);
            // construct our phantom reference to the collectable reference
            // that will actually trigger the cleanup
            new CollectableReferenceCleanup(ret, mark, allCollections.add(mark));
            return ret;
        }
    };

    // referenced only in threadCollection so we can track its lifetime in CollectableReferenceCleanup
    static final class CollectableReference<V>
    {
        final SimpleCollection<V> collection;
        CollectableReference(SimpleCollection<V> collection)
        {
            this.collection = collection;
        }
    }

    // marked finished by CollectableReferenceCleanup
    static final class MarkableReference<V>
    {
        final SimpleCollection<V> collection;
        volatile boolean finished;
        MarkableReference(SimpleCollection<V> collection)
        {
            this.collection = collection;
        }
    }

    static final class CollectableReferenceCleanup extends PhantomReference<CollectableReference<?>>
    {
        final MarkableReference<?> mark;
        final SimpleCollection.Item removeSelf;
        final SimpleCollection.Item removeCollection;
        CollectableReferenceCleanup(CollectableReference<?> referent, MarkableReference mark, SimpleCollection<?>.Item removeCollection)
        {
            super(referent, CLEANUP_QUEUE);
            this.mark = mark;
            this.removeCollection = removeCollection;
            this.removeSelf = CLEANUP_COLLECTION.add(this);
        }
        void cleanup()
        {
            removeSelf.delete();
            mark.finished = true;
            if (mark.collection.isEmpty())
                removeCollection.delete();
        }
    }

    public SimpleCollection.Item add(V v)
    {
        return threadCollection.get().collection.addExclusive(v);
    }

    public void register()
    {
        threadCollection.get();
    }

    public Iterator<V> iterator()
    {
        return new Iterator<V>()
        {
            Iterator<MarkableReference<V>> stripesIterator = allCollections.iterator();
            Iterator<V> cur = Collections.emptyIterator();
            public boolean hasNext()
            {
                if (cur.hasNext())
                    return true;
                while (true)
                {
                    if (!stripesIterator.hasNext())
                        return false;
                    MarkableReference<V> next = stripesIterator.next();
                    if ((cur = next.collection.iterator()).hasNext())
                        return true;
                    if (next.finished)
                        stripesIterator.remove();
                }
            }

            public V next()
            {
                if (!cur.hasNext() && !hasNext())
                    throw new NoSuchElementException();
                return cur.next();
            }
        };
    }

    private static final ReferenceQueue<Object> CLEANUP_QUEUE = new ReferenceQueue<>();
    private static final SimpleCollection<CollectableReferenceCleanup> CLEANUP_COLLECTION = new SimpleCollection<>();
    static
    {
        Thread cleanup = new Thread(new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    Reference<?> ref = CLEANUP_QUEUE.poll();
                    ((CollectableReferenceCleanup) ref).cleanup();
                }
            }
        });
        cleanup.start();
    }
}
