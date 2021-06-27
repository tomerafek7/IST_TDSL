import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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
    protected HashMap<ISTNode, ISTNode> ISTWriteSet = new HashMap<>();
    protected HashMap<ISTNode, ISTNode> ISTInverseWriteSet = new HashMap<>();  // maintaining a map of newNode -> oldNode
    protected HashSet<ISTNode> ISTReadSet = new HashSet<>();
    protected ArrayList<ISTNode> decActiveList = new ArrayList<>();
    protected ArrayList<ISTNode> incUpdateList = new ArrayList<>();


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

    protected void ISTPutIntoWriteSet(ISTNode node, boolean isToInner, List<ISTNode> childrenList, Integer key, Object val, boolean isEmpty) {
//        ISTWriteElement we = new ISTWriteElement();
        ISTNode newNode;
        if(isToInner){ // single --> inner
            newNode = new ISTNode(childrenList, childrenList.size());
        } else { // single --> single
            newNode = new ISTNode(key, val, isEmpty);
        }

        if(ISTInverseWriteSet.containsKey(node)){ // the old node is a replaced one
            ISTNode tempNode = ISTInverseWriteSet.get(node);
            ISTWriteSet.replace(tempNode, newNode); // override old Node
            // updating the inverse map
            ISTInverseWriteSet.remove(node);
            ISTInverseWriteSet.put(newNode, tempNode);
        } else { // normal case
            ISTWriteSet.put(node, newNode);
            ISTInverseWriteSet.put(newNode, node); // maintaining an inverse map of newNode -> oldNode
        }
//        we.isToInner = isToInner;
//        we.childrenList = childrenList;
//        we.key = key;
//        we.val = val;
//        we.isEmpty = isEmpty;
    }

    protected ISTNode ISTPutIntoReadSet(ISTNode node) {
        if(node.getVersion() > readVersion){ // abort immediately
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        ISTReadSet.add(node);
        // if this node is in the cur TX write-set - read it from there
        if(ISTWriteSet.containsKey(node)) {
            return ISTWriteSet.get(node);
        }
//            }ISTNode newNode = ISTWriteSet.get(node);
//            if(we.isToInner){ // single -> inner
//                // FIXME
//                ISTNode newNode = new ISTNode(we.childrenList, we.childrenList.size());
//                ISTWriteSet.replace()
//                return
//            } else { // single --> single
//                // FIXME
//                return new ISTNode(we.key, we.val, we.isEmpty);
//            }
//        }
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
