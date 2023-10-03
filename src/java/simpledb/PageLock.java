package simpledb;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * PageLock represents a shared or exclusive reentrant lock on a single page. It locks on the
 * transaction level, meaning a second thread acting on behalf of the same transaction the lock is
 * locked on will not block when trying to acquire the lock.
 */
public class PageLock {
    private static final int T_MIN = 50, T_RANGE = 400;
    private boolean shared; // false if the lock is exclusive
    private final Set<TransactionId> holders; // The transactions which hold this lock
    private final Random r;

    public PageLock() {
        r = new Random();
        this.shared = true;
        holders = new HashSet<>();
    }

    /**
     * Acquires this lock on behalf of transaction tid as a shared lock, implicitly converting from
     * exclusive if the lock isn't held. Otherwise, when the lock is exclusive and held, this
     * method blocks.
     * @param tid the transaction to acquire the lock on behalf of
     */
    synchronized void acquireShared(TransactionId tid) throws TransactionAbortedException {
        long endTime = System.currentTimeMillis() + T_MIN + r.nextInt(T_RANGE);
        while (!shared && holders.size() != 0) {
            lockWait(endTime);
        }
        holders.add(tid);
        shared = true;
        notifyAll();
    }

    /**
     * Acquires this lock as an exclusive lock, implicitly converting from shared if the lock
     * isn't held by any transactions or if the lock is held only by the transaction tid,
     * "upgrading" it. Blocks otherwise.
     * @param tid the transaction to acquire the lock on behalf of
     */
    synchronized void acquireExclusive(TransactionId tid) throws TransactionAbortedException {
        long endTime = System.currentTimeMillis() + T_MIN + r.nextInt(T_RANGE);
        while (!(isHeldBy(tid) && holders.size() == 1) && holders.size() != 0) {
            lockWait(endTime);
        }
        holders.add(tid);
        shared = false;
        notifyAll();
    }

    private synchronized void lockWait(long endTime) throws TransactionAbortedException {
        notifyAll();
        if (System.currentTimeMillis() > endTime) {
            throw new TransactionAbortedException();
        }
        try {
            wait(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Thread Interrupted");
        }
    }

    /**
     * Returns true if this lock is held by any transaction and false otherwise.
     */
    synchronized boolean isHeld() {
        return holders.size() > 0;
    }

    /**
     * Returns true if this lock is held by specified transaction and false otherwise.
     * @param tid the transaction to check if this lock is held by
     */
    synchronized boolean isHeldBy(TransactionId tid) {
        return holders.contains(tid);
    }

    /**
     * Releases this lock.
     * @param tid the transaction to release the lock on behalf of
     */
    synchronized void release(TransactionId tid) {
        holders.remove(tid);
        notifyAll();
    }
}
