import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class LocalStorage {

    protected long readVersion = 0L;
    protected long writeVersion = 0L; // for debug
    protected long TxNum = 0L; // to maintain order between TXs - IST
    protected boolean isTX = false;
    protected boolean readOnly = true;
    protected boolean earlyAbort = false;
    protected HashMap<Queue, LocalQueue> queueMap = new HashMap<Queue, LocalQueue>();
    protected HashMap<LNode, WriteElement> writeSet = new HashMap<LNode, WriteElement>();
    protected HashSet<LNode> readSet = new HashSet<LNode>();
    protected HashMap<LinkedList, ArrayList<LNode>> indexAdd = new HashMap<LinkedList, ArrayList<LNode>>();
    protected HashMap<LinkedList, ArrayList<LNode>> indexRemove = new HashMap<LinkedList, ArrayList<LNode>>();
    // IST:
    protected BiMap<ISTNode, ISTNode> ISTWriteSet = HashBiMap.create();
    protected HashSet<ISTNode> ISTReadSet = new HashSet<>();
    protected HashSet<ISTNode> decActiveList = new HashSet<>();
    protected ArrayList<ISTNode> incUpdateList = new ArrayList<>();
    protected int debugClear = 1;
    protected long tid = Thread.currentThread().getId();
    protected Stopwatch stopwatchTx;
    protected Stopwatch stopwatchWaiting;


    // with ArrayList all nodes will be added to the list
    // (no compression needed)
    // later, when we add the nodes to the index,
    // the latest node that was added to this list
    // will be the last update to the node in the index

    protected void putIntoWriteSet(LNode node, LNode next, Object val, boolean deleted) {
        WriteElement we = new WriteElement();
        we.next = next;
        we.deleted = deleted;
        we.val = val;
        writeSet.put(node, we);
        readOnly = false;
    }

    protected void ISTPutIntoWriteSet(ISTNode node, boolean isToInner, List<ISTNode> childrenList, Integer key, Object val, boolean isEmpty) {
        ISTNode fakeNode; // it is fake because we just use its single/inner, not the entire node (just need the struct for convenience)
        if (isToInner) { // single --> inner
            fakeNode = new ISTNode(childrenList, childrenList.size());
        } else { // single --> single
            fakeNode = new ISTNode(key, val, isEmpty);
        }
        ISTNode origin = ISTWriteSet.inverse().get(node);
        ISTNode prev;
        if(origin != null){ // this node is a value in WriteSet HashMap - we want to replace it, while keeping the same key.
            prev = ISTWriteSet.put(origin, fakeNode); // insert into Hash Map and get result.
        } else { // normal case
            prev = ISTWriteSet.put(node, fakeNode); // insert into Hash Map and get result.
        }
        assert prev == null || !prev.isInner; // this key could be inside before only if it was a single.
        readOnly = false;
    }

    protected ISTNode ISTGetUpdatedNodeFromWriteSet(ISTNode node){
        if(ISTWriteSet.containsKey(node)) {
            return ISTWriteSet.get(node);
        } else {
            return node;
        }
    }

    protected void ISTPutIntoReadSet(ISTNode node) {
        if (node.getVersion() > readVersion) { // abort immediately
            earlyAbort = true;
            TX.print("middle abort - localStorage");
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        ISTReadSet.add(node);
    }

    protected void addToIndexAdd(LinkedList list, LNode node) {
        ArrayList<LNode> nodes = indexAdd.get(list);
        if (nodes == null) {
            nodes = new ArrayList<LNode>();
        }
        nodes.add(node);
        indexAdd.put(list, nodes);
    }

    protected void addToIndexRemove(LinkedList list, LNode node) {
        ArrayList<LNode> nodes = indexRemove.get(list);
        if (nodes == null) {
            nodes = new ArrayList<LNode>();
        }
        nodes.add(node);
        indexRemove.put(list, nodes);
    }

}
