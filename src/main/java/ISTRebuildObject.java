import java.util.ArrayList;
import java.lang.Math;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;

public class ISTRebuildObject {
    ISTNode oldIstTree;
    AtomicReference<ISTNode>newIstTreeReference;
    ISTNode newIstTree;
    int index;//TODO: check if needed
    ISTNode parent;
    boolean finishedRebuild;
    ReentrantLock lock;


    ISTRebuildObject(ISTNode oldTree, int indexInParent, ISTNode parentNode){
        oldIstTree = oldTree;
        index = indexInParent;
        parent = parentNode;
        finishedRebuild = false;
        newIstTreeReference = null;
        newIstTree = null;
        ReentrantLock lock = new ReentrantLock();

    }
    static final int MIN_TREE_LEAF_SIZE = 4; //TODO: might fight a better value
    static final int COLLABORATION_THRESHOLD = 60; //TODO: might fight a better value


    public ISTNode buildIdealISTree(List<ISTNode> KVPairList) {
        int numOfLeaves = KVPairList.size();
        if (numOfLeaves <= MIN_TREE_LEAF_SIZE) {
            ISTNode myNode = new ISTNode(KVPairList, numOfLeaves);
//            myNode.children = new ArrayList<ISTNode>();
//            myNode.children.add(KVPairList.get(0));
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

    public ArrayList<ISTNode> createKVPairsList(ArrayList<ISTNode> list,
                                                   ISTNode currentNode, int numKeysToSkip, int numKeysToAdd) {
        for (int i = 0; i < currentNode.inner.numOfChildren; i++) {
            ISTNode child = currentNode.inner.children.get(i);
            if (!child.isInner  && (!child.single.isEmpty)) {//child is single and not empty
                if (numKeysToSkip > 0) {
                    numKeysToSkip--;
                } else { //here we found a single node that should be added.
                    list.add(child);
                    if (--numKeysToAdd == 0) return list;

                }
            } else  { //child is inner node
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
        //System.out.println("child index: " + index + ", KVPairsList: " + List.toString());
        ISTNode child = buildIdealISTree(List);
        if (index != 0){
            int key =  List.get(0).single.key;
            newIstTree.inner.keys.set(index -1, key);
            if (index == 1) {
                newIstTree.minKey = key;
            } else if(index == totalChildren -1){
                newIstTree.maxKey = key;
            }
        }
        //TODO: might be a rebuild above us, can optimize
        if (lock.tryLock()) { // if someone else caught the lock this rebuilt sub tree is no longer necessary
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
        if (keyCount < COLLABORATION_THRESHOLD) {
            ArrayList<ISTNode> list = new ArrayList<>();
            list = createKVPairsList(list, oldIstTree,0, keyCount);
            tempNewIstTree = buildIdealISTree(list);
        }
        else {
            tempNewIstTree = new ISTNode((int)Math.floor(Math.sqrt((double) keyCount)), keyCount);
        }
        newIstTreeReference.compareAndSet(null,tempNewIstTree);
        newIstTree = newIstTreeReference.get();


       if (keyCount > COLLABORATION_THRESHOLD) {
           ArrayList<ReentrantLock> locks= new ArrayList<>(index);
           while (true) {
               int index = newIstTree.inner.waitQueueIndex.getAndIncrement() ;
               if (index == newIstTree.inner.numOfChildren) break; // each child has a thread working on it;
               locks.set(index, new ReentrantLock());
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
        ISTNode innerCurNode = curNode;
        if (innerCurNode.inner.numOfChildren > COLLABORATION_THRESHOLD) {
            while (true) { // work queue
                int index = innerCurNode.inner.waitQueueIndex.getAndIncrement();
                if (index >= innerCurNode.inner.numOfChildren) break;
                subTreeCount(innerCurNode.inner.children.get(index));
            }
        }
        int keyCount = 0;
        for (ISTNode child : innerCurNode.inner.children){
            if (!child.isInner){
                keyCount += (child.single).isEmpty ? 0 : 1;
            } else { // inner
                ISTInnerNode innerChild = child.inner;
                //TODO: maor: i think we're good enough here, need to verify
                // TODO: need to read count & finished atomically!
                boolean finished = innerChild.finishedCount;
                if(finished) {
                    keyCount += innerChild.numOfLeaves;
                } else{
                  keyCount += subTreeCount(child);
                }
            }
        }
        innerCurNode.inner.numOfLeaves = keyCount; // TODO: need to update both fields atomically!
        innerCurNode.inner.finishedCount = true;

        return keyCount;
    }
    boolean helpRebuild(){
        if (finishedRebuild){
            return false; // in this case we need to update the root (outside)
        }
        int keyCount = subTreeCount(oldIstTree);
        createIdealCollaborative(keyCount);
        if (lock.tryLock()){//TODO: maor - maybe we can remove this lock because we newIstTree is atomic and won't be changed
            try {
                if (parent.inner.children.get(index) == oldIstTree) {
                    parent.inner.children.set(index, newIstTree); // DCSS(p.children[op.index], op, ideal, p.status, [0,⊥,⊥])
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
}
