package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    /**
     * 读取一个 entry，注意判断下可见性即可
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    /**
     * 插入数据，将数据包裹成entry，交给DM进行插入即可
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        // 包裹成entry交给dm处理
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    /**
     * 删除数据版本链中的一个版本，设置XMAX即可
     * 实际上主要是前置的三件事：一是可见性判断，二是获取资源的锁，三是版本跳跃判断。设置了XMAX则这个
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);                       // 添加到死锁检测
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);         // 自动回滚
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    /**
     * 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用：
     * @param level 隔离等级
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            // 开启一个新事务
            long xid = tm.begin();
            // 初始化事务的结构
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            // 将其存放在 activeTransaction 中，用于检查和快照使用
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，修改 TM 状态
     * @param xid
     * @throws Exception
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    // 手动回滚
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 自动回滚
     *
     * @param xid 事务id
     * @param autoAborted 是否自动回滚
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    // 释放Entry缓存
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
    
}
