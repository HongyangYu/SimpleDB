package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool which check that the transaction has the appropriate locks
 * to read/write the page.
 */

public class BufferPool {
	
	class MyPage{
		Page page;
		long visitTime;
		MyPage(Page page, long time){
			this.page = page;
			visitTime = time;
		}
	}
	
	/** Bytes per page, excluding header. */
	public static final int PAGE_SIZE = 4096;
	public static final int DEFAULT_PAGES = 100;
	public static final int DEFAULT_POLICY = 0;
	public static final int LRU_POLICY = 1;
	public static final int MRU_POLICY = 2;

	int replace_policy = DEFAULT_POLICY;

	int _numhits = 0;
	int _nummisses = 0;
	// @@
	int bufferSize;
	private static AtomicLong timeStamp = new AtomicLong(Long.MIN_VALUE+1);
	Page buffer[];
	Map<PageId, MyPage> bufferMap;

	/**
	 * Constructor.
	 *
	 * @param numPages
	 *            number of pages in this buffer pool
	 */
	public BufferPool(int numPages) {
		// TODO: IMPLEMENT THIS
		if (numPages <= 0) {
			numPages = DEFAULT_PAGES;
		}
		_numhits = 0;
		_nummisses = 0;
		bufferSize = numPages;
		buffer = new Page[numPages];
		bufferMap = new HashMap<PageId, MyPage>();
//		System.out.println("bufferSize = "+bufferSize);//##
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException, IOException {
		// TODO: IMPLEMENT THIS
		//@@ Do I need to use permission while getPage? 
		Page curPage;
		if (bufferMap.containsKey(pid)) {// the page is in the buffer.
//			System.out.println("getPage -hit " +pid.pageno());//##
			_numhits++;
			bufferMap.get(pid).visitTime = timeStamp.incrementAndGet();
			curPage =  bufferMap.get(pid).page;
		} else { // the page is not in the buffer.
//			System.out.println("getPage -miss " +pid.pageno());//##
			_nummisses++;
			int index = bufferMap.size();
			if (bufferMap.size() >= bufferSize) {
				index = evictPage();
//				System.out.println("getPage -evictPage " +index);//##
			} 
			DbFile dbfile = Database.getCatalog().getDbFile(pid.tableid());
			curPage = dbfile.readPage(pid);
			bufferMap.put(pid, new MyPage(curPage, timeStamp.incrementAndGet()));
			buffer[index] = curPage;
		}
		curPage.pin(); //pin_count + 1
		return curPage;
	}

	/**
	 * Pin page. Increment pin_count. If the pin_count was 0 before the call,
	 * the page was a candidate for replacement, but is no longer a candidate.
	 *
	 * @param index
	 *            the index of the page in the buffer pool
	 */
	public void pinPage(int index) {
		// TODO: IMPLEMENT THIS
		buffer[index].pin();
	}

	/**
	 * Unpin page. Unpin the page in the buffer pool. Should be called with
	 * dirty==TRUE if the user has modified the page. If so, this call should
	 * set the dirty bit for this frame. Furthermore, if pin_count>0, should
	 * decrement it. If pin_count==0 before this call, throws DbException.
	 *
	 * @param index
	 *            the index of the page in the buffer pool
	 * @param tid
	 *            the ID of the transaction unpinning the page
	 * @param dirty
	 *            the status of dirty_bit of the page (true or false)
	 */
	public void unpinPage(int index, TransactionId tid, boolean dirty) throws DbException, IOException {
		// TODO: IMPLEMENT THIS
		int pin_count = buffer[index].pin_count();
		if (pin_count == 0)
			throw new DbException("Error: Fail to unpin. pin_count=0");
		buffer[index].unpin();
		if(dirty) {// && buffer[index].isDirty()==null
			buffer[index].markDirty(dirty, tid);
			try {
				flushPage(buffer[index].id());
			} catch (IOException e) {
				throw new DbException("Error: Flush page error");
			}
		}
//			@@ Actually, I want flushPage while evictPage()
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public synchronized void releasePage(TransactionId tid, PageId pid) {
		// no need to implement this

	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public synchronized void transactionComplete(TransactionId tid) throws IOException {
		// no need to implement this
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
		// no need to implement this
		return false;
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public synchronized void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// no need to implement this
	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to. May block if the
	 * lock cannot be acquired.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// no need to implement this

	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is added to. May block if the lock cannot be
	 * acquired.
	 *
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param t
	 *            the tuple to add
	 */
	public synchronized void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// no need to implement this

	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// no need to implement this
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// no need to implement this
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid - an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		System.out.println("(flushPage()) Flushes page to disk");
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// no need to implement this
	}

	

	private synchronized int evictPage_LRU() throws DbException {
		long min = Long.MAX_VALUE;
		PageId outPid = null;
		for(Map.Entry<PageId, MyPage> entry : bufferMap.entrySet()){
			MyPage myPage = entry.getValue();
			if(myPage.visitTime < min  && myPage.page.pin_count()==0){//@@  && myPage.page.pin_count()==0
				min = myPage.visitTime;
				outPid = entry.getKey();
			}
		}
		if(outPid==null) throw new DbException("Error: Buffer pool is full and no page could be evicted");
		int index = -1;
		for(int i=0; i<bufferMap.size(); i++){
			if(buffer[i].id()==outPid){
				index = i;
				break;
			}
		}
		bufferMap.remove(outPid);
		return index;
	}
	

	private synchronized int evictPage_MRU() throws DbException {
		long max = Long.MIN_VALUE;
		PageId outPid = null;
		for(Map.Entry<PageId, MyPage> entry : bufferMap.entrySet()){
			MyPage myPage = entry.getValue();
			if(myPage.visitTime > max && myPage.page.pin_count()==0){ //@@  && myPage.page.pin_count()==0
				max = myPage.visitTime;
				outPid = entry.getKey();
			}
		}
		if(outPid==null) throw new DbException("Error: Buffer pool is full and no page could be evicted");
		int index = -1;
		for(int i=0; i<bufferSize; i++){
			if(buffer[i].id()==outPid){
				index = i;
				break;
			}
		}
		bufferMap.remove(outPid);
		return index;
	}
	
	/**
	 * Discards a page from the buffer pool. Return index of discarded page
	 */
	
	private synchronized int evictPage() throws DbException {
		// TODO: IMPLEMENT THIS
		if (replace_policy == 0) { // DEFAULT_POLICY  //Task A: do nothing, assuming buffer is infinite.
			return evictPage_LRU(); // Task C: do LRU or MRU
		} else if (replace_policy == 1) { // LRU_POLICY
			return evictPage_LRU();
		} else if (replace_policy == 2) { // MRU_POLICY
			return evictPage_MRU();
		} else {
			throw new DbException("Error: Policy undefined.");
		}
	}

	public int getNumHits() {
		return _numhits;
	}

	public int getNumMisses() {
		return _nummisses;
	}

	public void setReplacePolicy(int replacement) {
		this.replace_policy = replacement;
	}

	public int getBufferIndex(HeapPageId pid) {
		for (int i = 0; i < this.buffer.length; i++) {
			if (this.buffer[i] == null) {
				return -1;
			}
			if (pid.equals((this.buffer[i].id()))) {
				return i;
			}
		}
		return -1;
	}

	public void printBufferPool() {
		for (int i = 0; i < this.buffer.length; i++) {
			if (this.buffer[i] == null) {
				break;
			}
			System.out.print(this.buffer[i].id().pageno() + ":" + this.buffer[i].pin_count() + " - "); //##
		}
		System.out.println();
	}
}
