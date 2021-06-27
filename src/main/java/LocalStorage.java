import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class LocalStorage {

    protected long readVersion = 0L;
    protected long writeVersion = 0L; // for debug
    protected boolean TX = false;
    protected boolean readOnly = true;
    protected HashMap<Queue, LocalQueue> queueMap = new HashMap<Queue, LocalQueue>();
    protected HashMap<LNode, WriteElement> writeSet = new HashMap<LNode, WriteElement>();
    protected HashSet<LNode> readSet = new HashSet<LNode>();
    protected HashMap<LinkedList, ArrayList<LNode>> indexAdd = new HashMap<LinkedList, ArrayList<LNode>>();
    protected HashMap<LinkedList, ArrayList<LNode>> indexRemove = new HashMap<LinkedList, ArrayList<LNode>>();
    // IST:
    protected HashMap<ISTNode, ISTSingleWriteElement> ISTSingleWriteSet = new HashMap<>();
    protected HashMap<ISTNode, ISTInnerWriteElement> ISTInnerWriteSet = new HashMap<>();
    protected HashSet<ISTNode> ISTReadSet = new HashSet<>();
    protected ArrayList<ISTInnerNode> decActiveList = new ArrayList<>();
    protected ArrayList<ISTInnerNode> incUpdateList = new ArrayList<>();


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
    }

    protected void ISTPutIntoSingleWriteSet(ISTNode node, Integer key, Object val, boolean isEmpty) {
        ISTSingleWriteElement we = new ISTSingleWriteElement();
        we.key = key;
        we.val = val;
        we.isEmpty = isEmpty;
        ISTSingleWriteSet.put(node, we);
    }

    protected void ISTPutIntoInnerWriteSet(ISTNode node, Integer index, ISTNode son) {
        ISTInnerWriteElement we = new ISTInnerWriteElement();
        we.index = index;
        we.son = son;
        ISTInnerWriteSet.put(node, we);
    }
    protected ISTNode ISTPutIntoReadSet(ISTNode node, ISTInnerNode parent, Integer index) {
        if(node.getVersion() > readVersion){ // abort immediately
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        ISTReadSet.add(node);
        // if this node is in the cur TX write-set - read it from there
        // 1. check if the parent is in write-set (changed ptr)
        if(ISTInnerWriteSet.containsKey(parent)) {
            ISTInnerWriteElement we = ISTInnerWriteSet.get(parent);
            if (index.equals(we.index)) { // relevant only if this is the same index
                return we.son;
            }
        }
        // 2. check if the node is in write-set (changed value / isEmpty)
        if(ISTSingleWriteSet.containsKey(node)) {
            ISTSingleWriteElement we = ISTSingleWriteSet.get(node);
            return new ISTSingleNode(we.key, we.val, we.isEmpty);
        }
        // not in write-set:
        return node;
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
