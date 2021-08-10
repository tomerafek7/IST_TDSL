import java.util.ArrayList;
import java.lang.Math;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;

public class ISTRebuildObject {
    ISTNode oldIstTree;
    AtomicReference<ISTNode> newIstTreeReference;
    ISTNode newIstTree;
    int indexInParent;//TODO: check if needed
    ISTNode parent;
    boolean finishedRebuild;
    ReentrantLock lock;
    AtomicReference<ArrayList<ReentrantLock>> childrenLocks;


    ISTRebuildObject(ISTNode oldTree, int indexInParent, ISTNode parentNode){
        oldIstTree = oldTree;
        this.indexInParent = indexInParent;
        parent = parentNode;
        finishedRebuild = false;
        newIstTreeReference = new AtomicReference<>(null);
        newIstTree = null;
        lock = new ReentrantLock();
        childrenLocks = new AtomicReference<>(null);
    }
    static final int MIN_TREE_LEAF_SIZE = 4; //TODO: might fight a better value
    static final int COLLABORATION_THRESHOLD = 60; //TODO: might fight a better value


    public ISTNode buildIdealISTree(List<ISTNode> KVPairList) {
        debugCheckKVPairsList(KVPairList);
        int numOfLeaves = KVPairList.size();
        if (numOfLeaves <= MIN_TREE_LEAF_SIZE) {
            ISTNode myNode = new ISTNode(KVPairList, numOfLeaves);
//            myNode.children = new ArrayList<ISTNode>();
//            myNode.children.add(KVPairList.get(0));
            return myNode;
        }
        debugCheckKVPairsList(KVPairList);
        int numChildren = (int) Math.floor(Math.sqrt(numOfLeaves));
        int childSize = (int) Math.floor((((double) numOfLeaves) / numChildren));
        int remainder = numOfLeaves % numChildren;
        ISTNode myNode = new ISTNode(numChildren, numOfLeaves);
        for (int i = 0; i < numChildren; i++) {
            int size = childSize + (i < remainder ? 1 : 0);
            debugCheckKVPairsList(KVPairList.subList(0, size));
            debugCheckKVPairsList(KVPairList);
            myNode.inner.children.set(i,buildIdealISTree(KVPairList.subList(0, size)));
            debugCheckKVPairsList(KVPairList);
            if (i != 0) {
                myNode.inner.keys.set(i-1, KVPairList.get(0).single.key);
                if (i == 1) myNode.minKey = KVPairList.get(0).single.key;
                if (i == numChildren - 1) myNode.maxKey = KVPairList.get(0).single.key;

            }
            debugCheckKVPairsList(KVPairList);
            KVPairList = KVPairList.subList(size, KVPairList.size());
            debugCheckKVPairsList(KVPairList);
        }
        return myNode;

    }

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

    boolean rebuildAndSetChild (int keyCount,int index, ReentrantLock lock) { //keyCount is of the parent node which initiated the rebuild
        int totalChildren = (int)Math.floor( Math.sqrt((double)keyCount));
        int childSize = Math.floorDiv(keyCount,totalChildren);
        int remainder = keyCount % totalChildren;
        int fromKey = childSize * index + Math.min(index,remainder);
        int childKeyCount = childSize + (index < remainder ? 1 : 0);
        ArrayList<ISTNode> List = new ArrayList<>();
        List = createKVPairsList(List, oldIstTree, fromKey, childKeyCount);
        debugCheckKVPairsList(List);
        //System.out.println("child index: " + index + ", KVPairsList: " + List.toString());
        ISTNode child = buildIdealISTree(List);
        if (index != 0){
            if (List.get(0).single == null){
                int x = 1;
            }
            int key =  List.get(0).single.key;
            newIstTree.inner.keys.set(index -1, key);
            if (index == 1) {
                newIstTree.minKey = key;
            } else if(index == totalChildren -1){
                newIstTree.maxKey = key;
            }
        }
        //TODO: might be a rebuild above us, can optimize
        if (true) { // if someone else caught the lock this rebuilt sub tree is no longer necessary
            lock.lock();
            try {
                if (newIstTree.inner.children.get(index) == null) {// TODO first : compare this with AtomicReference
                    newIstTree.inner.children.set(index, child);
                }
            } finally {
                lock.unlock();
            }
        }
        return true;
    }

    void createIdealCollaborative(int keyCount) {
        ISTNode tempNewIstTree;
        if (keyCount <= COLLABORATION_THRESHOLD) {
            ArrayList<ISTNode> list = new ArrayList<>();
            list = createKVPairsList(list, oldIstTree,0, keyCount);
            debugCheckKVPairsList(list);
            tempNewIstTree = buildIdealISTree(list);
        }
        else {
            tempNewIstTree = new ISTNode((int)Math.floor(Math.sqrt((double) keyCount)), keyCount);
        }
        newIstTreeReference.compareAndSet(null,tempNewIstTree);
        newIstTree = newIstTreeReference.get();


       if (keyCount > COLLABORATION_THRESHOLD) { // collaboration case
           // each thread creates lock list
           ArrayList<ReentrantLock> locks = new ArrayList<>();
           for (int i=0; i<newIstTree.inner.numOfChildren; i++){ // init array as nulls
               locks.add(new ReentrantLock());
           }
           // only one thread manages to insert his list
           childrenLocks.compareAndSet(null, locks);
           locks = childrenLocks.get();
           while (true) {
               int index = newIstTree.inner.waitQueueIndex.getAndIncrement() ;
               if (index >= newIstTree.inner.numOfChildren) break; // each child has a thread working on it;
               //locks.set(index, new ReentrantLock()); // not needed because we set the locks above
               rebuildAndSetChild(keyCount, index, locks.get(index));
           }
           for (int i=0; i<newIstTree.inner.numOfChildren; i ++){
               ISTNode child = newIstTree.inner.children.get(i);
               if (child == null) { //can happen only in multi treading- 1 of the threads did not finish
                   if ( ! rebuildAndSetChild(keyCount,i, locks.get(i))) {
                       return; //TODO maoras: maybe not needed
                   }
               }
           }
       }

    }

    public int subTreeCount(ISTNode curNode){

        if (!curNode.isInner){
            return (curNode.single).isEmpty ? 0 : 1;
        }
        // here node is inner
        if (curNode.inner.numOfChildren > COLLABORATION_THRESHOLD) {
            while (true) { // work queue
                int index = curNode.inner.waitQueueIndex.getAndIncrement();
                if (index >= curNode.inner.numOfChildren) break;
                subTreeCount(curNode.inner.children.get(index));
            }
        }
        int keyCount = 0;
        for (ISTNode child : curNode.inner.children){
            if (!child.isInner){
                keyCount += (child.single).isEmpty ? 0 : 1;
            } else { // inner
                //TODO: maor: i think we're good enough here, need to verify
                // TODO: need to read count & finished atomically!
                boolean finished = child.inner.finishedCount;
                if(finished) {
                    keyCount += child.inner.numOfLeaves;
                } else{
                  keyCount += subTreeCount(child);
                }
            }
        }
        curNode.inner.numOfLeaves = keyCount; // TODO: need to update both fields atomically!
        curNode.inner.finishedCount = true;

        return keyCount;
    }
    boolean helpRebuild(){
        if (finishedRebuild){
            return false; // in this case we need to update the root (outside)
        }
        int keyCount = subTreeCount(oldIstTree);
        if(keyCount == 0){ // corner case - rebuild is done with 0 leaves (all are empty) - so the new root is single
            newIstTreeReference.compareAndSet(null, new ISTNode(null, null, true)); // empty single
            newIstTree = newIstTreeReference.get();
        } else {
            createIdealCollaborative(keyCount);
        }
        if (lock.tryLock()){//TODO: maor - maybe we can remove this lock because we newIstTree is atomic and won't be changed
            try {
                if (parent.inner.children.get(indexInParent) == oldIstTree) {
                    if(newIstTree.inner == null){
                        int x = 1;
                    }
                    parent.inner.children.set(indexInParent, newIstTree); // DCSS(p.children[op.index], op, ideal, p.status, [0,⊥,⊥])
                    finishedRebuild = true;
                }
            }
            finally {
                lock.unlock();
            }
        }
        if (IST.DEBUG_MODE){
          //  IST.debugPrintNumLeaves(newIstTree);

        }
        return true;
    }

    public void debugCheckKVPairsList(List<ISTNode> list){
        for(ISTNode node: list){
            if(node.single == null){
                int x = 1;
            }
        }
    }
}
