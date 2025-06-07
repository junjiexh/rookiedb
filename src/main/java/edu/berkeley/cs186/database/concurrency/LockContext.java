package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        if (readonly) {
            throw new UnsupportedOperationException("不能在readonly的resource上进行acquire, resource=" + getResourceName());
        }

        LockType currentLock = getExplicitLockType(transaction);
        if (LockType.NL.equals(lockType)) {
            throw new InvalidLockException("不能acquire NL lock，use release instead");
        }
        if (Objects.equals(currentLock, lockType)) {
            throw new DuplicateLockRequestException("acquire failed, 重复获取锁, lock=" + lockType);
        }

        // 两种情况不能进行acquire
        // 1. 还没获得对应意向锁（父节点上）
        // 2. 祖父节点已持有SIX锁，请求在子孙节点上获取 IS 或 S 锁
        LockContext parentContext = parentContext();
        if (parentContext != null && !LockType.canBeParentLock(parentContext.getExplicitLockType(transaction), lockType)) {
            throw new InvalidLockException("不能在已经获取了lock=" + parentContext.getExplicitLockType(transaction)
                    + "的父节点的子节点上获取" + lockType);
        }
        if (hasSIXAncestor(transaction) && (LockType.IS.equals(lockType) || LockType.S.equals(lockType))) {
            throw new InvalidLockException("不能在祖父节点已持有SIX锁时获取lock, lock=" + lockType);
        }
        lockman.acquire(transaction, getResourceName(), lockType);

        // 更新父节点的子锁计数
        updateParentChildLockNum(transaction, CHILD_ACQUIRE);
    }

    public static final int CHILD_ACQUIRE = 1;
    public static final int CHILD_RELEASE = -1;
    private void updateParentChildLockNum(TransactionContext transaction, int add) {
        LockContext parentContext = parentContext();
        if (parentContext == null) {
            return;
        }
        parentContext.numChildLocks.compute(transaction.getTransNum(), (k, v) -> v == null ? add : v + add);
    }
    private void updateParentChildLockNum(ResourceName resourceName, TransactionContext transaction, int add) {
        LockContext lockContext = fromResourceName(lockman, resourceName);
        LockContext parentContext = lockContext.parentContext();
        if (parentContext == null) {
            return;
        }
        parentContext.numChildLocks.compute(transaction.getTransNum(), (k, v) -> v == null ? add : v + add);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("不能在readonly的resource上进行acquire, resource=" + getResourceName());
        }

        LockType currentLock = getExplicitLockType(transaction);
        if (LockType.NL.equals(currentLock)) {
            throw new NoLockHeldException("当前transaction在资源resource=" + name + " 上未持有锁");
        }

        // NL不能成为子节点的父节点
        List<Lock> descendantLocks = lockman.getDescendantLocks(transaction, getResourceName());
        for (Lock descendantLock : descendantLocks) {
            if (!LockType.canBeParentLock(LockType.NL, descendantLock.lockType)) {
                throw new InvalidLockException("release failed, 子节点仍持有lock=" + descendantLock.lockType + ", 不能释放lock=" + currentLock);
            }
        }

        lockman.release(transaction, getResourceName());
        updateParentChildLockNum(transaction, CHILD_RELEASE);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("不能在readonly的resource上进行promote, resource=" + getResourceName());
        }
        LockType currentLock = getExplicitLockType(transaction);
        if (LockType.NL.equals(currentLock)) {
            throw new NoLockHeldException("promote failed, resource=" + name + " 上未持有锁");
        }
        if (newLockType.equals(currentLock)) {
            throw new DuplicateLockRequestException("promote failed, 已持有目标lock, required=" + newLockType);
        }

        // 进入invalid state的情况：
        // - 不是promotion
        // - 与父亲节点的状态冲突，比如父亲是IS，当前是S -> X
        // - 如果需要提升到SIX
        //      - 父节点不能是SIX
        //      - 需要同时释放子节点的所有S和IS
        if (!LockType.substitutable(newLockType, currentLock)) {
            throw new InvalidLockException("promote failed, 不能将lock1=" + currentLock + "替换为lock2=" + newLockType);
        }
        LockType parentLock = parentContext() != null ? parentContext().getExplicitLockType(transaction) : null;
        if (parentLock != null && !LockType.canBeParentLock(parentLock, newLockType)) {
            throw new InvalidLockException("promote failed, new lock=" + currentLock + " 不能成为parent lock=" + parentLock + "的孩子");
        }
        // SIX promote的特殊处理
        if (LockType.SIX.equals(newLockType)) {
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("promote failed, target lock=" + newLockType + ", 但是祖先节点中已有SIX锁");
            }
            List<ResourceName> descendants = sisDescendants(transaction);
            descendants.add(name); // 同时释放自己的锁
            lockman.acquireAndRelease(transaction, name, newLockType, descendants); // 保证原子性
            for (ResourceName descendant : descendants) {
                updateParentChildLockNum(descendant, transaction, CHILD_RELEASE);
            }
        } else {
            // 非SIX情况
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (readonly) {
            throw new UnsupportedOperationException("不能在readonly的resource上进行promote, resource=" + getResourceName());
        }

        LockType currentLevelLock = getExplicitLockType(transaction);
        if (LockType.NL.equals(currentLevelLock)) {
            throw new NoLockHeldException("escalate failed, transaction=" + transaction.getTransNum() + " doesn't held any lock at resource=" + getResourceName());
        }

        List<Lock> descendantLocks = lockman.getDescendantLocks(transaction, getResourceName());
        List<ResourceName> releasedResources = descendantLocks.stream().map(l -> l.name).collect(Collectors.toList());

        // 不允许升级到意向锁（IS/IX/SIX）
        boolean needX = LockType.IX.equals(currentLevelLock) || LockType.SIX.equals(currentLevelLock);
        Optional<Lock> XLikeLock = descendantLocks.stream()
                .filter(l -> LockType.IX.equals(l.lockType) || LockType.SIX.equals(l.lockType) || LockType.X.equals(l.lockType))
                .findAny();
        if (XLikeLock.isPresent()) {
            needX = true;
        }

        // 幂等校验
        if (currentLevelLock.equals(needX ? LockType.X : LockType.S)) {
            return;
        }

        releasedResources.add(this.name); // 因为是升级锁，所以当前resource也要释放
        if (needX) {
            lockman.acquireAndRelease(transaction, this.name, LockType.X, releasedResources);
        } else {
            lockman.acquireAndRelease(transaction, this.name, LockType.S, releasedResources);
        }
        for (ResourceName releasedResource : releasedResources) {
            updateParentChildLockNum(releasedResource, transaction, CHILD_RELEASE);
        }

        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        return lockman.getLocks(transaction).stream()
                .filter(lock -> name.equals(lock.name))
                .map(lock -> lock.lockType)
                .findAny().orElse(LockType.NL);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;

        LockType explicitLockType = getExplicitLockType(transaction);
        if (!LockType.NL.equals(explicitLockType)) {
            return explicitLockType;
        }

        LockContext parent = parentContext();
        while (null != parent) {
            LockType pl = parent.getExplicitLockType(transaction);
            if (LockType.S.equals(pl) || LockType.X.equals(pl)) {
                return pl;
            }
            // IX部分不能继承给子节点，因为不一定是在该子节点的孩子中进行写操作
            if (LockType.SIX.equals(pl)) {
                return LockType.S;
            }
            parent = parent.parentContext();
        }

        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext parentContext = parentContext();
        while (parentContext != null) {
            if (LockType.SIX.equals(parentContext.getExplicitLockType(transaction))) {
               return true;
            }
            parentContext = parentContext.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        // 需要的锁：父节点有当前节点，且lock是S或者IS
        return locks.stream()
                .filter(lock -> lock.name.isDescendantOf(this.name))
                .filter(lock -> LockType.S.equals(lock.lockType) || LockType.IS.equals(lock.lockType))
                .map(lock -> lock.name)
                .collect(Collectors.toList());
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

