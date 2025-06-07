package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // 当前锁已经满足需求，do nothing
        // NL会包含在里面
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // 如果当前锁已经持有IX，且请求一个S锁，则会将其升级为SIX
        if (LockType.IX.equals(explicitLockType) && LockType.S.equals(requestType)) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        // 如果当前已经持有意向锁
        // 例如 IX -> X, IS -> S
        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            return;
        }

        // 如果都不是，考虑的情况：
        // current: NL，required: S/X -> acquire
        // current: S, required: X -> promote
        ensureParentLockHeld(transaction, lockContext, requestType == LockType.X ? LockType.IX : LockType.IS);
        if (LockType.NL.equals(effectiveLockType)) {
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }

        return;
    }

    /**
     * 确保当前锁的父锁持有了所需的锁类型。
     * 递归向上检查，从最上层的锁开始检查。
     * 如果父锁不满足要求，则为其获取所需的锁类型。
     *
     * @param current current lock context
     * @param requiredLockType 需要的锁类型，只能是IX或者IS
     */
    public static void ensureParentLockHeld(TransactionContext transaction, LockContext current, LockType requiredLockType) {
        if (!requiredLockType.equals(LockType.IX) && !requiredLockType.equals(LockType.IS)) {
            throw new IllegalArgumentException("Lock type must be IX or IS, not " + requiredLockType);
        }

        LockContext parent = current.parentContext();
        if (parent == null) {
            return;
        }
        ensureParentLockHeld(transaction, parent, requiredLockType);

        LockType parentLock = parent.getEffectiveLockType(transaction);
        // 父锁已经满足要求，可以不用获取
        if (LockType.substitutable(parentLock, requiredLockType)) {
            return;
        }

        if (LockType.NL.equals(parentLock)) {
            parent.acquire(transaction, requiredLockType);
            return;
        }

        parent.promote(transaction, requiredLockType);
    }
}
