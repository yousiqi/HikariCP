/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zaxxer.hikari.util;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 * <p>
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @param <T> the templated type to store in the bag
 * @author Brett Wooldridge
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

    // 负责存放ConcurrentBag中全部用于借出去的资源
    private final CopyOnWriteArrayList<T> sharedList;
    // 是否弱引用标记
    private final boolean weakThreadLocals;

    // 用于加速本地资源访问，避免线程交互
    private final ThreadLocal<List<Object>> threadList;
    private final IBagStateListener listener;
    private final AtomicInteger waiters;
    private volatile boolean closed;

    // 用于存在资源等待线程时的第一手资源交接
    private final SynchronousQueue<T> handoffQueue;

    public interface IConcurrentBagEntry {
        int STATE_NOT_IN_USE = 0;
        int STATE_IN_USE = 1;
        int STATE_REMOVED = -1;
        int STATE_RESERVED = -2;

        boolean compareAndSet(int expectState, int newState);

        void setState(int newState);

        int getState();
    }

    public interface IBagStateListener {
        void addBagItem(int waiting);
    }

    /**
     * Construct a ConcurrentBag with the specified listener.
     *
     * @param listener the IBagStateListener to attach to this bag
     */
    public ConcurrentBag(final IBagStateListener listener) {
        this.listener = listener;
        this.weakThreadLocals = useWeakThreadLocals();

        this.handoffQueue = new SynchronousQueue<>(true);
        this.waiters = new AtomicInteger();
        this.sharedList = new CopyOnWriteArrayList<>();
        if (weakThreadLocals) {
            this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
        } else {
            this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
        }
    }

    /**
     * 从bag中借用BagEntry资源
     *
     * @param timeout  how long to wait before giving up, in units of unit
     * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
     * @return a borrowed instance from the bag or null if a timeout occurs
     * @throws InterruptedException if interrupted while waiting
     */
    public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException {
        // 优先在thread-local中查看有没可用资源
        final List<Object> list = threadList.get();
        for (int i = list.size() - 1; i >= 0; i--) {
            final Object entry = list.remove(i);
            @SuppressWarnings("unchecked") final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
            if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                return bagEntry;
            }
        }

        // 当无可用本地化资源时，查看sharedList是否存在可用资源
        final int waiting = waiters.incrementAndGet();
        try {
            for (T bagEntry : sharedList) {
                if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    // If we may have stolen another waiter's connection, request another bag add.
                    if (waiting > 1) {
                        listener.addBagItem(waiting - 1);
                    }
                    return bagEntry;
                }
            }

            // 因为可能“抢走”了其他线程的资源，因此提醒包裹进行资源添加
            listener.addBagItem(waiting);

            timeout = timeUnit.toNanos(timeout);
            do {
                final long start = currentTime();
                // 当现有资源全部在使用的时候，等待handoffQueue一个被释放的资源或一个新的资源
                final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
                if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    return bagEntry;
                }

                timeout -= elapsedNanos(start);
            } while (timeout > 10_000);

            return null;
        } finally {
            waiters.decrementAndGet();
        }
    }

    /**
     * 还回一个借出去的对象，如果只有借出去borrowed()而没有调用还requite()方法，则会导致内存泄漏
     *
     * @param bagEntry the value to return to the bag
     * @throws NullPointerException  if value is null
     * @throws IllegalStateException if the bagEntry was not borrowed from the bag
     */
    public void requite(final T bagEntry) {
        // 将状态设置为未使用
        bagEntry.setState(STATE_NOT_IN_USE);

        // 判断是否存在等待线程，若存在则直接将资源给它
        for (int i = 0; waiters.get() > 0; i++) {
            if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
                return;
            } else if ((i & 0xff) == 0xff) {
                parkNanos(MICROSECONDS.toNanos(10));
            } else {
                Thread.yield();
            }
        }

        // 否则进行资源本地化到ThreadLocal
        final List<Object> threadLocalList = threadList.get();
        if (threadLocalList.size() < 50) {
            threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
        }
    }

    /**
     * 添加一个对象，所以的资源仅能通过此方法添加
     *
     * @param bagEntry 添加的对象
     */
    public void add(final T bagEntry) {
        if (closed) {
            LOGGER.info("ConcurrentBag has been closed, ignoring add()");
            throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
        }

        // 新加入的资源优先放入CopyOnWriteArrayList
        sharedList.add(bagEntry);

        // 当有等待资源的线程时，将资源交到某个等待线程（handoffQueue.offer(bagEntry)）后才返回
        while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
            Thread.yield();
        }
    }

    /**
     * 从bag中取出一个值
     *
     * @param bagEntry the value to remove
     * @return true if the entry was removed, false otherwise
     * @throws IllegalStateException if an attempt is made to remove an object
     *                               from the bag that was not borrowed or reserved first
     */
    public boolean remove(final T bagEntry) {
        // 如果资源正在使用且无法进行状态转换，则返回失败
        if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
            LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
            return false;
        }

        // 从CopyOnWriteArrayList中移出
        final boolean removed = sharedList.remove(bagEntry);
        if (!removed && !closed) {
            LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
        }

        // 从当前线程资源中移除
        threadList.get().remove(bagEntry);

        return removed;
    }

    /**
     * Close the bag to further adds.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * This method provides a "snapshot" in time of the BagEntry
     * items in the bag in the specified state.  It does not "lock"
     * or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in list before performing any action on them.
     *
     * @param state one of the {@link IConcurrentBagEntry} states
     * @return a possibly empty list of objects having the state specified
     */
    public List<T> values(final int state) {
        final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    /**
     * This method provides a "snapshot" in time of the bag items.  It
     * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in the list, or understand the concurrency implications of
     * modifying items, before performing any action on them.
     *
     * @return a possibly empty list of (all) bag items
     */
    @SuppressWarnings("unchecked")
    public List<T> values() {
        return (List<T>) sharedList.clone();
    }

    /**
     * The method is used to make an item in the bag "unavailable" for
     * borrowing.  It is primarily used when wanting to operate on items
     * returned by the <code>values(int)</code> method.  Items that are
     * reserved can be removed from the bag via <code>remove(T)</code>
     * without the need to unreserve them.  Items that are not removed
     * from the bag can be make available for borrowing again by calling
     * the <code>unreserve(T)</code> method.
     *
     * @param bagEntry the item to reserve
     * @return true if the item was able to be reserved, false otherwise
     */
    public boolean reserve(final T bagEntry) {
        return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
    }

    /**
     * This method is used to make an item reserved via <code>reserve(T)</code>
     * available again for borrowing.
     *
     * @param bagEntry the item to unreserve
     */
    @SuppressWarnings("SpellCheckingInspection")
    public void unreserve(final T bagEntry) {
        if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
            // spin until a thread takes it or none are waiting
            while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
                Thread.yield();
            }
        } else {
            LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
        }
    }

    /**
     * Get the number of threads pending (waiting) for an item from the
     * bag to become available.
     *
     * @return the number of threads waiting for items from the bag
     */
    public int getWaitingThreadCount() {
        return waiters.get();
    }

    /**
     * Get a count of the number of items in the specified state at the time of this call.
     *
     * @param state the state of the items to count
     * @return a count of how many items in the bag are in the specified state
     */
    public int getCount(final int state) {
        int count = 0;
        for (IConcurrentBagEntry e : sharedList) {
            if (e.getState() == state) {
                count++;
            }
        }
        return count;
    }

    public int[] getStateCounts() {
        final int[] states = new int[6];
        for (IConcurrentBagEntry e : sharedList) {
            ++states[e.getState()];
        }
        states[4] = sharedList.size();
        states[5] = waiters.get();

        return states;
    }

    /**
     * Get the total number of items in the bag.
     *
     * @return the number of items in the bag
     */
    public int size() {
        return sharedList.size();
    }

    public void dumpState() {
        sharedList.forEach(entry -> LOGGER.info(entry.toString()));
    }

    /**
     * 根据在该类和系统类加载器之间是否有一个自定义类加载器实现来确定是否使用WeakReferences。
     *
     * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
     */
    private boolean useWeakThreadLocals() {
        try {
            // 人工指定是否使用弱引用，但官方不推荐自主配置
            if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
                return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
            }

            // 通过判断初始化的ClassLoader是否是系统的ClassLoader来确定是否弱引用
            return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
        } catch (SecurityException se) {
            return true;
        }
    }
}
