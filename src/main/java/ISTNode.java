import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ISTNode {
    boolean wasLocal;
    Integer minKey; // minKey in the keys of the node ONLY
    Integer maxKey; // maxKey in the keys of the node ONLY
    ISTInnerNode inner;
    ISTSingleNode single;
    boolean isInner;
    // lock & version parameters: (copied from LNode)
    private static final long lockMask = 0x1000000000000000L;
    private static final long deleteMask = 0x2000000000000000L;
    private static final long singletonMask = 0x4000000000000000L;
    private static final long versionNegMask = lockMask | deleteMask | singletonMask;
    private AtomicLong versionAndFlags = new AtomicLong();

    public ISTNode(List<ISTNode> childrenList, int leaves){
        inner = new ISTInnerNode(childrenList, leaves);
        single = null;
        isInner = true;
        minKey = inner.keys.get(0);
        maxKey = inner.keys.get(inner.keys.size()-1);
        wasLocal = false;
    }

    public ISTNode(int numOfChildrenReceived, int leaves){
        minKey = 0;
        maxKey = 0;
        inner = new ISTInnerNode(numOfChildrenReceived, leaves);
        single = null;
        isInner = true;
    }

    public ISTNode(Integer key, Object value, boolean isEmpty){
        inner = null;
        single = new ISTSingleNode(key, value, isEmpty);
        isInner = false;
        this.minKey = key;
        this.maxKey = key;
    }

    // lock methods copied from LNode:

    protected boolean tryLock() {
        long l = versionAndFlags.get();
        if ((l & lockMask) != 0) {
            return false;
        }
        long locked = l | lockMask;
        return versionAndFlags.compareAndSet(l, locked);
    }

    protected void unlock() {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);
        long unlocked = l & (~lockMask);
        boolean ret = versionAndFlags.compareAndSet(l, unlocked);
        assert (ret);
    }

    protected boolean isLocked() {
        long l = versionAndFlags.get();
        return (l & lockMask) != 0;
    }

    // version methods copied from LNode:

    protected long getVersion() {
        return (versionAndFlags.get() & (~versionNegMask));
    }

    protected void setVersion(long version) {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);
        l &= versionNegMask;
        l |= (version & (~versionNegMask));
        versionAndFlags.set(l);
    }

    // increment counter for node. if it meets the rebuild conditions AND no other TX reached the threshold, "sign" it with TxNum.
    protected void incrementRebuildCounter(long TxNum) {
        int count = this.inner.updateCount++;
        if (count > (this.inner.numOfLeaves * IST.rebuildUpdatesRatioThreshold) && count > IST.rebuildMinUpdatesThreshold &&
                this.inner.needRebuildVersion == -1L) {
            this.inner.needRebuildVersion = TX.TxCounter.get();
        }
    }


}

