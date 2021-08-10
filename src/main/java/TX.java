import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TX {

    public static final boolean DEBUG_MODE_LL = false;
    public static final boolean DEBUG_MODE_QUEUE = false;
    private static final boolean DEBUG_MODE_TX = false;
    private static final boolean DEBUG_MODE_VERSION = false;

    protected static ThreadLocal<LocalStorage> lStorage = ThreadLocal.withInitial(LocalStorage::new);

    private static AtomicLong GVC = new AtomicLong(); // original GVC
    private static AtomicLong TxCounter = new AtomicLong(); // used to maintain order between TXs - IST

    // stats:
    public static AtomicInteger abortCount = new AtomicInteger(0);

    protected static long getVersion() {
        return GVC.get();
    }

    protected static long incrementAndGetVersion() {
        return GVC.incrementAndGet();
    }

    protected static long incrementAndGetTxCounter() {
        return TxCounter.incrementAndGet();
    }

    public static void TXbegin() {

        if (DEBUG_MODE_TX) {
            System.out.println("TXbegin");
        }

        LocalStorage localStorage = lStorage.get();
        localStorage.TX = true;
        localStorage.readVersion = getVersion();
        localStorage.TxNum = incrementAndGetTxCounter();
    }

    public static boolean TXend() throws TXLibExceptions.AbortException {

        if (DEBUG_MODE_TX) {
            System.out.println("TXend");
        }

        boolean abort = false;

        LocalStorage localStorage = lStorage.get();

        if (!localStorage.TX) {
            if (DEBUG_MODE_TX) {
                System.out.println("TXend - abort the TX");
            }
            abort = true;
        }

        // locking write set

        HashMap<LNode, WriteElement> writeSet = localStorage.writeSet;

        HashSet<LNode> lockedLNodes = new HashSet<>();

        if (!abort) {

            for (Entry<LNode, WriteElement> entry : writeSet.entrySet()) {
                LNode node = entry.getKey();
                if (!node.tryLock()) {
                    abort = true;
                    break;
                }
                lockedLNodes.add(node);
            }

        }
        // locking queues
        HashMap<Queue, LocalQueue> qMap = localStorage.queueMap;

        if (!abort) {

            for (Entry<Queue, LocalQueue> entry : qMap.entrySet()) {

                Queue queue = entry.getKey();

                if (!queue.tryLock()) { // if queue is locked by another thread
                    abort = true;
                    break;

                }

                LocalQueue lQueue = entry.getValue();
                lQueue.isLockedByMe = true;

            }
        }

        // locking IST write set

        HashMap<ISTNode, ISTNode> ISTWriteSet = localStorage.ISTWriteSet;

        HashSet<ISTNode> lockedISTNodes = new HashSet<>();

        if (!abort) {
            for ( Entry<ISTNode, ISTNode> entry : ISTWriteSet.entrySet()) {
                ISTNode node = entry.getKey();
                if (!node.tryLock()) {
                    abort = true;
                    break;
                }
                lockedISTNodes.add(node);
            }
        }

        // validate read set

        HashSet<LNode> readSet = localStorage.readSet;

        if (!abort) {

            for (LNode node : readSet) {
                if (!lockedLNodes.contains(node) && node.isLocked()) {
                    // someone else holds the lock
                    abort = true;
                    break;
                } else if (node.getVersion() > localStorage.readVersion) {
                    abort = true;
                    break;
                } else if (node.getVersion() == localStorage.readVersion && node.isSingleton()) {
                    incrementAndGetVersion(); // increment GVC
                    node.setSingleton(false);
                    if (DEBUG_MODE_VERSION) {
                        System.out.println("singleton - increment GVC");
                    }
                    abort = true;
                    break;
                }

            }

        }

        // validate queue

        if (!abort) {

            for (Entry<Queue, LocalQueue> entry : qMap.entrySet()) {

                Queue queue = entry.getKey();
                if (queue.getVersion() > localStorage.readVersion) {
                    abort = true;
                    break;
                } else if (queue.getVersion() == localStorage.readVersion && queue.isSingleton()) {
                    incrementAndGetVersion(); // increment GVC
                    abort = true;
                    break;
                }

            }

        }

        // validate IST read set

        HashSet<ISTNode> ISTReadSet = localStorage.ISTReadSet;

        if (!abort) {

            for (Object o : ISTReadSet) {
                ISTNode node = (ISTNode)o;
                if (!lockedISTNodes.contains(node) && node.isLocked()) {
                    // someone else holds the lock
                    abort = true;
                    break;
                } else if (node.getVersion() > localStorage.readVersion) {
                    abort = true;
                    break;
                }
            }
        }


        // increment GVC

        long writeVersion = 0;

        if (!abort && !localStorage.readOnly) {
            writeVersion = incrementAndGetVersion();
            assert (writeVersion > localStorage.readVersion);
            localStorage.writeVersion = writeVersion;
        }

        // commit

        if (!abort && !localStorage.readOnly) {
            // LinkedList

            for (Entry<LNode, WriteElement> entry : writeSet.entrySet()) {
                LNode node = entry.getKey();
                WriteElement we = entry.getValue();

                node.next = we.next;
                node.val = we.val; // when node val changed because of put
                if (we.deleted) {
                    node.setDeleted(true);
                    node.val = null; // for index
                }
                node.setVersion(writeVersion);
                node.setSingleton(false);
            }
        }

        if (!abort) {
            // Queue

            for (Entry<Queue, LocalQueue> entry : qMap.entrySet()) {

                Queue queue = entry.getKey();
                LocalQueue lQueue = entry.getValue();

                queue.dequeueNodes(lQueue.nodeToDeq);
                queue.enqueueNodes(lQueue);
                if (TX.DEBUG_MODE_QUEUE) {
                    System.out.println("commit queue before set version");
                }
                queue.setVersion(writeVersion);
                queue.setSingleton(false);

            }

        }

        if (!abort) {
            // IST
            for (Entry<ISTNode, ISTNode> entry : ISTWriteSet.entrySet()) {
//                ISTWriteElement we = entry.getValue();
                ISTNode newNode = entry.getValue();
                ISTNode node = entry.getKey();
                if(newNode.isInner) { // single --> inner
                    node.changeToInner(newNode.inner);
                } else { // single --> single
                    node.single.key = newNode.single.key;
                    node.single.value = newNode.single.value;
                    node.single.isEmpty = newNode.single.isEmpty;
                    node.setVersion(writeVersion);
                }
            }
        }

        // release locks, even if abort

        lockedLNodes.forEach(LNode::unlock);
        lockedISTNodes.forEach(ISTNode::unlock);

        for (Entry<Queue, LocalQueue> entry : qMap.entrySet()) {

            Queue queue = entry.getKey();
            LocalQueue lQueue = entry.getValue();
            if (lQueue.isLockedByMe) {
                queue.unlock();
                lQueue.isLockedByMe = false;
            }
        }

        // TODO: should we do something with index for IST?

        // update index
        if (!abort && !localStorage.readOnly) {
            // adding to index
            HashMap<LinkedList, ArrayList<LNode>> indexMap = localStorage.indexAdd;
            for (Entry<LinkedList, ArrayList<LNode>> entry : indexMap.entrySet()) {
                LinkedList list = entry.getKey();
                ArrayList<LNode> nodes = entry.getValue();
                nodes.forEach(node -> list.index.add(node));
            }
            // removing from index
            indexMap = localStorage.indexRemove;
            for (Entry<LinkedList, ArrayList<LNode>> entry : indexMap.entrySet()) {
                LinkedList list = entry.getKey();
                ArrayList<LNode> nodes = entry.getValue();
                nodes.forEach(node -> list.index.remove(node));
            }
        }

        // IST - Fetch-And-Add
        for(ISTNode node : localStorage.decActiveList){
            node.inner.activeTX.decrementAndGet();
            // node.inner.activeThreadsSet.remove(localStorage.tid); // cleaning this TX from all relevant nodes
        }
        for(ISTNode node : localStorage.incUpdateList){
            node.incrementRebuildCounter(localStorage.TxNum);
        }

        // cleanup

        localStorage.queueMap.clear();
        localStorage.writeSet.clear();
        localStorage.readSet.clear();
        // IST
        localStorage.ISTWriteSet.clear();
        localStorage.ISTInverseWriteSet.clear();
        localStorage.ISTReadSet.clear();
        localStorage.decActiveList.clear();
        localStorage.incUpdateList.clear();
        // IST end
        localStorage.indexAdd.clear();
        localStorage.indexRemove.clear();

        localStorage.TX = false;
        localStorage.readOnly = true;

        if (DEBUG_MODE_TX) {
            if (abort) {
                System.out.println("TXend - aborted");
            }
            System.out.println("TXend - is done");
        }

        if (abort) {
            abortCount.incrementAndGet();
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        return true;

    }

}
