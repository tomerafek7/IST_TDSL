import java.util.ArrayList;
import java.lang.Math;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;

public class ISTRebuildObject {
    ISTInnerNode oldIstTree;
    AtomicReference<ISTInnerNode>newIstTreeReference;
    ISTInnerNode newIstTree;
    int index;//TODO: check if needed
    ISTInnerNode parent;
    boolean finishedRebuild;
    ReentrantLock lock;


    ISTRebuildObject(ISTInnerNode oldTree, int indexInParent, ISTInnerNode parentNode){
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


    public ISTInnerNode buildIdealISTree(List<ISTSingleNode> KVPairList) {
        int numOfLeaves = KVPairList.size();
        if (numOfLeaves <= MIN_TREE_LEAF_SIZE) {
            ISTInnerNode myNode = new ISTInnerNode(KVPairList, numOfLeaves);
//            myNode.children = new ArrayList<ISTNode>();
//            myNode.children.add(KVPairList.get(0));
            return myNode;
        }
        int numChildren = (int) Math.floor(Math.sqrt(numOfLeaves));
        int childSize = (int) Math.floor((((double) numOfLeaves) / numChildren));
        int remainder = numOfLeaves % numChildren;
        ISTInnerNode myNode = new ISTInnerNode(numChildren, numOfLeaves);
        for (int i = 0; i < numChildren; i++) {
            int size = childSize + (i < remainder ? 1 : 0);
            myNode.children.set(i,buildIdealISTree(KVPairList.subList(0, size)));
            if (i != 0) {
                myNode.keys.set(i-1, KVPairList.get(0).key);
                if (i == 1) myNode.minKey = KVPairList.get(0).key;
                if (i == numChildren - 1) myNode.maxKey = KVPairList.get(0).key;

            }
            KVPairList = KVPairList.subList(size, KVPairList.size());
        }
        return myNode;

    }

    public ArrayList<ISTSingleNode> createKVPairsList(ArrayList<ISTSingleNode> list,
                                                   ISTInnerNode currentNode, int numKeysToSkip, int numKeysToAdd) {
        for (int i = 0; i < currentNode.numOfChildren; i++) {
            ISTNode child = currentNode.children.get(i);
            if (child instanceof ISTSingleNode && (!((ISTSingleNode) child).isEmpty)) {
                if (numKeysToSkip > 0) {
                    numKeysToSkip--;
                } else { //here we found a single node that should be added.
                    list.add((ISTSingleNode) child);
                    if (--numKeysToAdd == 0) return list;

                }
            } else if (child instanceof ISTInnerNode) { //child is inner node
                if (numKeysToSkip > ((ISTInnerNode) child).numOfLeaves) {
                    numKeysToSkip -= ((ISTInnerNode) child).numOfLeaves;
                } else if (numKeysToSkip + numKeysToAdd <= ((ISTInnerNode) child).numOfLeaves) { // all of the children are in this subtree
                    return createKVPairsList(list, ((ISTInnerNode) child), numKeysToSkip, numKeysToAdd);
                } else { // some of the children are in the next subtree
                    list = createKVPairsList(list, ((ISTInnerNode) child), numKeysToSkip, numKeysToAdd);
                    numKeysToAdd -= ((ISTInnerNode) child).numOfLeaves - numKeysToSkip;
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
        ArrayList<ISTSingleNode> List = new ArrayList<>();
        List = createKVPairsList(List, oldIstTree, fromKey, childKeyCount);
        //System.out.println("child index: " + index + ", KVPairsList: " + List.toString());
        ISTInnerNode child = buildIdealISTree(List);
        if (index != 0){
            int key =  List.get(0).key;
            newIstTree.keys.set(index -1, key);
            if (index == 1) {
                newIstTree.minKey = key;
            } else if(index == totalChildren -1){
                newIstTree.maxKey = key;
            }
        }
        //TODO: might be a rebuild above us, can optimize
        if (lock.tryLock()) { // if someone else caught the lock this rebuilt sub tree is no longer necessary
            if (newIstTree.children.get(index) == null) {// TODO first : compare this with AtomicReference
                newIstTree.children.set(index, child);
            }
            lock.unlock();
        }
        return true;
    }

    void createIdealCollaborative(int keyCount) {
        ISTInnerNode tempNewIstTree;
        if (keyCount < COLLABORATION_THRESHOLD) {
            ArrayList<ISTSingleNode> list = new ArrayList<>();
            list = createKVPairsList(list, oldIstTree,0, keyCount);
            tempNewIstTree = buildIdealISTree(list);
        }
        else {
            tempNewIstTree = new ISTInnerNode((int)Math.floor(Math.sqrt((double) keyCount)), keyCount);
        }
        newIstTreeReference.compareAndSet(null,tempNewIstTree);
        newIstTree = newIstTreeReference.get();


       if (keyCount > COLLABORATION_THRESHOLD) {
           ArrayList<ReentrantLock> locks= new ArrayList<>(index);
           while (true) {
               int index = newIstTree.waitQueueIndex.getAndIncrement() ;
               if (index == newIstTree.numOfChildren) break; // each child has a thread working on it;
               locks.set(index, new ReentrantLock());
               rebuildAndSetChild(keyCount, index, locks.get(index));
           }
           for (int i=0; i<newIstTree.numOfChildren; i ++){
               ISTNode child = newIstTree.children.get(i);
               if (child == null) { //can happen only in multi treading- 1 of the threads did not finish
                   if ( ! rebuildAndSetChild(keyCount,i, locks.get(i))) {
                       return; //TODO maoras: maybe not needed
                   }
               }
           }
       }

    }

    public int subTreeCount(ISTNode curNode){

        if (curNode instanceof ISTSingleNode){
            return ((ISTSingleNode) curNode).isEmpty ? 0 : 1;
        }
        // here node is inner
        ISTInnerNode innerCurNode = (ISTInnerNode)curNode;
        if (innerCurNode.numOfChildren > COLLABORATION_THRESHOLD) {
            while (true) { // work queue
                int index = innerCurNode.waitQueueIndex.getAndIncrement();
                if (index >= innerCurNode.numOfChildren) break;
                subTreeCount(innerCurNode.children.get(index));
            }
        }
        int keyCount = 0;
        for (ISTNode child : innerCurNode.children){
            if (child instanceof ISTSingleNode){
                keyCount += ((ISTSingleNode) child).isEmpty ? 0 : 1;
            } else { // inner
                ISTInnerNode innerChild = (ISTInnerNode)child;
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
        innerCurNode.numOfLeaves = keyCount; // TODO: need to update both fields atomically!
        innerCurNode.finishedCount = true;

        return keyCount;
    }
    boolean helpRebuild(){
        if (finishedRebuild){
            return false; // in this case we need to update the root (outside)
        }
        int keyCount = subTreeCount(oldIstTree);
        createIdealCollaborative(keyCount);
        if (lock.tryLock()){
            if (parent.children.get(index) == oldIstTree) {
                parent.children.set(index,newIstTree); // DCSS(p.children[op.index], op, ideal, p.status, [0,⊥,⊥])
            }
            lock.unlock();
        }
        if (IST.DEBUG_MODE){
          //  IST.debugPrintNumLeaves(newIstTree);

        }
        return true;
    }
}
