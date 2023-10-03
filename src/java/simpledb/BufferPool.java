package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pages;
    private final Queue<PageId> usageQueue;
    final private Map<PageId, PageLock> lockBook;
    final private Map<TransactionId, Set<PageId>> transactionPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>(numPages);
        this.usageQueue = new ConcurrentLinkedQueue<>();
        this.lockBook = new ConcurrentHashMap<>();
        this.transactionPages = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() { return pageSize; }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        transactionPages.putIfAbsent(tid, new HashSet<>());
        getLock(tid, pid, perm);
        transactionPages.get(tid).add(pid);

        Page ret = pages.get(pid);
        if (ret != null) { return ret; }

        DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        pages.put(pid, databaseFile.readPage(pid));
        if (pages.size() > numPages)
            evictPage();
        usageQueue.add(pid);
        return pages.get(pid);
    }

    private void getLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        lockBook.putIfAbsent(pid, new PageLock());
        PageLock lock = lockBook.get(pid);
        if (perm == Permissions.READ_WRITE) {
            lock.acquireExclusive(tid);
        } else if (!lock.isHeldBy(tid)) {
            // Will only block if different transaction tries to read. If the transaction seeking
            // to read already holds the lock, even exclusively, the lock need not be acquired.
            lock.acquireShared(tid);
        }
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockBook.containsKey(pid) && lockBook.get(pid).isHeldBy(tid);
    }

    /**
     * Releases the lock on a page. If page isn't locked, silently does nothing.
     * Calling this is very risky, and may result in wrong behavior. Think hard about who needs to
     * call this and why, and why they can run the risk of calling it.
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        if (lockBook.containsKey(pid)) {
            lockBook.get(pid).release(tid);
        }
    }

    /**
     * Commit a given transaction. Release all locks associated with the transaction.
     * @param tid the ID of the transaction requesting completion
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction. Has no effect if called twice
     * @param tid the ID of the transaction requesting completion
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        //System.out.printf("T%03d: Finished. Abort=%B\n", tid.myid, !commit); //!debug
        // Dummy entry bc some tests circumvent getPage() so transactionPages.get(tid)==null.
        transactionPages.putIfAbsent(tid, new HashSet<>());
        for (PageId pid : transactionPages.get(tid)) {
            if (pages.containsKey(pid)) {
                Page page = pages.get(pid);
                if (commit) {
                    // Allows redoing if pages of committed transaction aren't flushed before crash
                    Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
                    Database.getLogFile().force();
                    page.setBeforeImage();
                } else { // Abort:
                    // LogFile.rollback discards pages so really nothing needs to be done here.
                }
            }
            // Release lock and removes lock if it is no longer being used
            PageLock lock = lockBook.get(pid);
            lock.release(tid);
            if (!lock.isHeld()) {
                lockBook.remove(pid);
            }
        }
        transactionPages.remove(tid);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        transactionPages.putIfAbsent(tid, new HashSet<>());
        for (PageId pid : transactionPages.get(tid))
            flushPage(pid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        List<Page> modified = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page modifiedPage : modified ) {
            modifiedPage.markDirty(true, tid);

            // Mostly just for tests to pass (I think). DbFile.insertTuple should bring page into
            // cache by calling BufferPool.getPage but HeapFileDuplicates in BufferPoolWriteTest
            // doesn't access pages this way.
            pages.put(modifiedPage.getId(), modifiedPage);

            usageQueue.remove(modifiedPage.getId());
            usageQueue.add(modifiedPage.getId());
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        List<Page> modified = Database.getCatalog().getDatabaseFile(t.getTableId()).deleteTuple(tid, t);
        for (Page modifiedPage : modified ) {
            modifiedPage.markDirty(true, tid);
            usageQueue.remove(modifiedPage.getId());
            usageQueue.add(modifiedPage.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pages.keySet())
            flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool and its LRU cache.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
        usageQueue.remove(pid);
        // No need to remove from transactionPages. The only purpose that would serve would
        // be preventing flushing of discarded pages
    }

    /**
     * Flushes a certain page to disk, and write corresponding log record.
     * If the page is clean does nothing.
     * @param pid an ID indicating the page to flush
     * @throws IllegalArgumentException when page given is dirty but locked
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = pages.get(pid);
        if (page == null) {
            // System.out.println("Attempted to flush nonexistent page"); !debug
            return;
        }

        // Write to disk if the page is dirty. Append a log record if transaction which dirtied it
        // is still running, as this flush might need to be undone. If the transaction is not
        // running but dirtied the page, it must have been committed (aborting discards pages).
        TransactionId dirtier = page.isDirty();
        if (dirtier != null ) {
            if (transactionPages.containsKey(dirtier)) {
                // Allows undoing if dirtying transaction is aborted or fails to commit.
                Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                Database.getLogFile().force();
            }
            table.writePage(page);
            page.markDirty(false, null); // mark page as clean now
        }

        // if clean do nothing
    }

    /**
     * Removes one page from the buffer pool or LRU cache and flushes the page to disk if necessary.
     */
    private synchronized void evictPage() throws DbException {
        PageId leastRecentlyUsed = usageQueue.peek(); // removed from queue by discardPage()
        if (leastRecentlyUsed == null) {
            throw new IllegalStateException("evictPage() called but no pages in LRU cache!");
        }
        try {
            flushPage(leastRecentlyUsed);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        discardPage(leastRecentlyUsed);
    }

}
