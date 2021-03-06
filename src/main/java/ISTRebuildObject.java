import java.util.ArrayList;
import java.lang.Math;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;

public class ISTRebuildObject {
    ISTNode oldIstTree;
    AtomicReference<ISTNode> newIstTreeReference;
    ISTNode newIstTree;
    int indexInParent;
    ISTNode parent;
    boolean finishedRebuild;
    ReentrantLock lock; // used for synchronizing which thread sets the new root after rebuild is done
    AtomicReference<ArrayList<ReentrantLock>> childrenLocksRebuild;
    AtomicReference<ArrayList<ReentrantLock>> childrenLocksCount;

    ISTRebuildObject(ISTNode oldTree, int indexInParent, ISTNode parentNode){
        oldIstTree = oldTree;
        this.indexInParent = indexInParent;
        parent = parentNode;
        finishedRebuild = false;
        newIstTreeReference = new AtomicReference<>(null);
        newIstTree = null;
        lock = new ReentrantLock();
        childrenLocksRebuild = new AtomicReference<>(null);
        childrenLocksCount = new AtomicReference<>(null);
    }

    // serial and recursive method
    // builds an ideally-shaped tree with all of the key-value pairs from KVPairList.
    public ISTNode buildIdealISTree(List<ISTNode> KVPairList) {
        if(TX.DEBUG_MODE_IST) {
            debugCheckKVPairsList(KVPairList);
        }
        int numOfLeaves = KVPairList.size();
        if (numOfLeaves <= IST.rebuildMinTreeLeafSize) {
            ISTNode myNode = new ISTNode(KVPairList, numOfLeaves);
            return myNode;
        }
        int numChildren = (int) Math.floor(Math.sqrt(numOfLeaves));
        int childSize = (int) Math.floor((((double) numOfLeaves) / numChildren));
        int remainder = numOfLeaves % numChildren;
        ISTNode myNode = new ISTNode(numChildren, numOfLeaves);
        for (int i = 0; i < numChildren; i++) {
            int size = childSize + (i < remainder ? 1 : 0);
            myNode.inner.children.set(i,buildIdealISTree(KVPairList.subList(0, size)));
            if (i != 0) {
                myNode.inner.keys.set(i-1, KVPairList.get(0).single.key);
                if (i == 1) myNode.minKey = KVPairList.get(0).single.key;
                if (i == numChildren - 1) myNode.maxKey = KVPairList.get(0).single.key;

            }
            KVPairList = KVPairList.subList(size, KVPairList.size());
        }
        return myNode;

    }

    // serial and recursive method
    // creates a list of all relevant (in range of (numKeysToSkip, numKeysToSkip + numKeysToAdd)),
    // by DFSing the old tree.
    public ArrayList<ISTNode> createKVPairsList(ArrayList<ISTNode> list,
                                                   ISTNode currentNode, int numKeysToSkip, int numKeysToAdd) {
        for (int i = 0; i < currentNode.inner.numOfChildren; i++) {
            ISTNode child = currentNode.inner.children.get(i);
            if (!child.isInner) { //child is single
                if (!child.single.isEmpty) { // child is not empty
                    if (numKeysToSkip > 0) {
                        numKeysToSkip--;
                    } else { //here we found a single node that should be added.
                        list.add(child);
                        if (--numKeysToAdd == 0) return list;
                    }
                }
            } else { //child is inner node
                if (numKeysToSkip > (child.inner).numOfLeaves) {
                    numKeysToSkip -= (child.inner).numOfLeaves;
                } else if (numKeysToSkip + numKeysToAdd <= (child.inner).numOfLeaves) { // all of the children are in this subtree
                    return createKVPairsList(list, (child), numKeysToSkip, numKeysToAdd);
                } else { // some of the children are in the next subtree
                    list = createKVPairsList(list, (child), numKeysToSkip, numKeysToAdd);
                    numKeysToAdd -= (child.inner).numOfLeaves - numKeysToSkip;
                    numKeysToSkip = 0;
                }
            }
        }
        return list;
    }

    // builds an ideal IST and try (it's a race) to write it at son #<index>
    boolean rebuildAndSetChild(int keyCount, int index, ReentrantLock lock) { // keyCount is of the parent node which initiated the rebuild

        int totalChildren = (int) Math.floor(Math.sqrt((double) keyCount));
        int childSize = Math.floorDiv(keyCount, totalChildren);
        int remainder = keyCount % totalChildren;
        int fromKey = childSize * index + Math.min(index, remainder);
        int childKeyCount = childSize + (index < remainder ? 1 : 0);
        ArrayList<ISTNode> List = new ArrayList<>();
        List = createKVPairsList(List, oldIstTree, fromKey, childKeyCount);
        if (TX.DEBUG_MODE_IST) {
            debugCheckKVPairsList(List);
        }
        ISTNode child = buildIdealISTree(List);
        if (index != 0) {
            int key = List.get(0).single.key;
            newIstTree.inner.keys.set(index - 1, key);
            if (index == 1) {
                newIstTree.minKey = key;
            } else if (index == totalChildren - 1) {
                newIstTree.maxKey = key;
            }
        }
        //TODO: might be a rebuild above us, can optimize
        if (true) { // if someone else caught the lock this rebuilt sub tree is no longer necessary
            if (lock.tryLock()) {
                try {
                    if (newIstTree.inner.children.get(index) == null) {
                        newIstTree.inner.children.set(index, child);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
        return true;
    }

    // main rebuild function - called from helpRebuild
    // decides whether the rebuild is done in parallel or serially (based on rebuildCollaborationThreshold constant)
    void createIdealCollaborative(int keyCount) {
        ISTNode tempNewIstTree;
        if (keyCount <= IST.rebuildCollaborationThreshold) {
            ArrayList<ISTNode> list = new ArrayList<>();
            list = createKVPairsList(list, oldIstTree,0, keyCount);
            if (TX.DEBUG_MODE_IST) {
                debugCheckKVPairsList(list);
            }
            tempNewIstTree = buildIdealISTree(list);
        }
        else {
            tempNewIstTree = new ISTNode((int)Math.floor(Math.sqrt((double) keyCount)), keyCount);
        }
        newIstTreeReference.compareAndSet(null,tempNewIstTree);
        newIstTree = newIstTreeReference.get();

       if (keyCount > IST.rebuildCollaborationThreshold) { // collaboration case
           // each thread creates lock list
           ArrayList<ReentrantLock> locks = new ArrayList<>();
           for (int i=0; i<newIstTree.inner.numOfChildren; i++){ // init array as nulls
               locks.add(new ReentrantLock());
           }
           // only one thread manages to insert his list
           childrenLocksRebuild.compareAndSet(null, locks);
           locks = childrenLocksRebuild.get();
           while (true) {
               int index = newIstTree.inner.waitQueueIndexRebuild.getAndIncrement() ;
               if (index >= newIstTree.inner.numOfChildren) break; // each child has a thread working on it;
               rebuildAndSetChild(keyCount, index, locks.get(index));
           }
           for (int i=0; i<newIstTree.inner.numOfChildren; i ++){
               ISTNode child = newIstTree.inner.children.get(i);
               if (child == null) { //can happen only in multi threading - 1 of the threads did not finish
                   if ( ! rebuildAndSetChild(keyCount,i, locks.get(i))) {
                       return; //TODO maoras: maybe not needed
                   }
               }
           }
       }

    }

    // counts the sub-tree of <subTreeRoot>
    // may do it parallel or serially (based on rebuildCollaborationThreshold constant)
    // non-recursive function (even if it's done in parallel - each thread will be in charge of counting one child)
    public int subTreeCount(ISTNode subTreeRoot){

        // create lock list
        ArrayList<ReentrantLock> locks = new ArrayList<>();
        for (int i=0; i<subTreeRoot.inner.numOfChildren; i++){ // init array as nulls
            locks.add(new ReentrantLock());
        }
        // only one thread manages to insert his list
        childrenLocksCount.compareAndSet(null, locks);
        locks = childrenLocksCount.get();

        if (subTreeRoot.inner.numOfChildren > IST.rebuildCollaborationThreshold) {
            while (true) { // work queue
                int index = subTreeRoot.inner.waitQueueIndexCount.getAndIncrement();
                if (index >= subTreeRoot.inner.numOfChildren) break;
                countAndSetChild(subTreeRoot.inner.children.get(index), locks.get(index));
            }
        }
        int keyCount = 0;
        for (int i=0; i<subTreeRoot.inner.children.size(); i++) {
            ISTNode child = subTreeRoot.inner.children.get(i);
            if (!child.isInner) {
                keyCount += (child.single).isEmpty ? 0 : 1;
            } else { // inner
                keyCount += countAndSetChild(child, locks.get(i));
            }
        }
        return keyCount;
    }

    // this function is in charge of synchronizing the locks so that the parallel counting will work fluently.
    // it calls countChild which performs the actual counting of the node's subtree.
    public int countAndSetChild(ISTNode node, ReentrantLock lock){
        lock.lock();
        if(!node.inner.finishedCount) {
            node.inner.numOfLeaves = countChild(node);
            node.inner.finishedCount = true;
        }
        lock.unlock();
        return node.inner.numOfLeaves;
    }

    // basic, recursive function which returns the num of (undeleted) leaves in <node>'s subtree.
    public int countChild(ISTNode node){
        if (!node.isInner){
            return (node.single).isEmpty ? 0 : 1;
        }
        int keyCount = 0;
        for (ISTNode child : node.inner.children){
            if (!child.isInner){
                keyCount += (child.single).isEmpty ? 0 : 1;
            } else { // inner
                keyCount += countChild(child);
            }
        }
        node.inner.numOfLeaves = keyCount;
        return keyCount;

    }

    // called from IST's rebuild function
    // Its main job is to handle the race of setting the new root (after the rebuild is done)
    // calls createIdealCollaborative which performs the rebuild.
    void helpRebuild(){
        if (finishedRebuild){
            return;
        }
        int keyCount = subTreeCount(oldIstTree);
        if(keyCount == 0){ // corner case - rebuild is done with 0 leaves (all are empty) - so the new root is single
            newIstTreeReference.compareAndSet(null, new ISTNode(null, null, true)); // empty single
            newIstTree = newIstTreeReference.get();
        } else {
            createIdealCollaborative(keyCount);
        }
        lock.lock();
        try {

            if (parent.inner.children.get(indexInParent) == oldIstTree) {
                parent.inner.children.set(indexInParent, newIstTree);
                finishedRebuild = true;
            }
        }
        finally {
            lock.unlock();
        }
    }

    // debug function...
    public void debugCheckKVPairsList(List<ISTNode> list){

        int last = 0;
        int i=0;
        for (ISTNode node : list){
            if (i>0) {
                if (node.single.key < last) {
                    StringBuilder error = new StringBuilder();
                    for (int j = Math.max(i-5,0); j< Math.min(i+5,list.size()); j++){
                        error.append(" ").append(list.get(j).single.key);
                    }
                    TX.print(error.toString());
                    assert false;
                }
            }
            i++;
            last = node.single.key;
        }
    }
}
