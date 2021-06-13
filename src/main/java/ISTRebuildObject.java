import java.util.ArrayList;
import java.lang.Math;
import java.util.List;

public class ISTRebuildObject<V> {
    ISTInnerNode<V> oldIstTree;
    ISTInnerNode<V> newIstTree;
    int index;//TODO: check if needed
    ISTInnerNode<V> parent;

    ISTRebuildObject(ISTInnerNode<V> oldTree, int indexInParent, ISTInnerNode<V> parentNode){
        oldIstTree = oldTree;
        index = indexInParent;
        parent = parentNode;

    }
    static final int MIN_TREE_LEAF_SIZE = 4; //TODO: might fight a better value
    static final int COLLABORATION_THRESHOLD = 60; //TODO: might fight a better value


    public ISTInnerNode<V> buildIdealISTree(List<ISTSingleNode<V>> KVPairList) {
        int numOfLeaves = KVPairList.size();
        if (numOfLeaves < MIN_TREE_LEAF_SIZE) {
            ISTInnerNode<V> myNode = new ISTInnerNode<V>(KVPairList);
            myNode.children = new ArrayList<ISTNode<V>>();
            myNode.children.add(KVPairList.get(0));
            return myNode;
        }
        int numChildren = (int) Math.floor(Math.sqrt(numOfLeaves));
        int childSize = (int) Math.floor((((double) numOfLeaves) / numChildren));
        int remainder = numOfLeaves % numChildren;
        ISTInnerNode<V> myNode = new ISTInnerNode<V>(numChildren);
        for (int i = 0; i < numChildren; i++) {
            int size = childSize + (i < remainder ? 1 : 0);
            myNode.children.add(buildIdealISTree(KVPairList.subList(0, size - 1)));
            if (i != 0) {
                myNode.keys.add(KVPairList.get(0).key);
                if (i == 1) myNode.minKey = KVPairList.get(0).key;
                if (i == numChildren - 1) myNode.maxKey = KVPairList.get(0).key;

            }
            KVPairList = KVPairList.subList(size, KVPairList.size() - 1);
        }
        return myNode;

    }

    public ArrayList<ISTSingleNode<V>> createKVPairsList(ArrayList<ISTSingleNode<V>> List,
                                                   ISTInnerNode<V> currentNode, int numKeysToSkip, int numKeysToAdd) {
        for (int i = 0; i < currentNode.numOfChildren; i++) {
            ISTNode<V> child = currentNode.children.get(i);
            if (child instanceof ISTSingleNode && (!((ISTSingleNode<V>) child).isEmpty)) {
                if (numKeysToSkip > 0) {
                    numKeysToSkip--;
                } else { //here we found a single node that should be added.
                    List.add((ISTSingleNode<V>) child);
                    if (--numKeysToAdd == 0) return List;

                }
            } else if (child instanceof ISTInnerNode) { //child is inner node
                if (numKeysToSkip > ((ISTInnerNode<V>) child).numOfLeaves) {
                    numKeysToSkip -= ((ISTInnerNode<V>) child).numOfLeaves;
                } else if (numKeysToSkip + numKeysToAdd < ((ISTInnerNode<V>) child).numOfLeaves) { //all of the children are in this subtree
                    List = createKVPairsList(List, ((ISTInnerNode<V>) child), numKeysToSkip, numKeysToAdd);
                    return List;

                } else { // some of the children are in the next subtree
                    List = createKVPairsList(List, ((ISTInnerNode<V>) child), numKeysToSkip, numKeysToAdd);
                    numKeysToAdd = numKeysToAdd - (((ISTInnerNode<V>) child).numOfLeaves - numKeysToSkip);
                    numKeysToSkip = 0;
                }
            }
        }
        return List;
    }

    boolean rebuildAndSetChild (int keyCount,int index) { //keyCount is of the parent node which initiated the rebuild
        int totalChildren = (int)Math.floor( Math.sqrt((double)keyCount));
        int childSize = Math.floorDiv(keyCount,totalChildren);
        int remainder = keyCount %totalChildren;
        int fromKey = childSize * index + Math.min(index,remainder);
        int childKeyCount = childSize + (index < remainder ? 1 : 0);
        ArrayList<ISTSingleNode<V>> List = new ArrayList<>();
        List = createKVPairsList(List, oldIstTree,fromKey,childKeyCount);
        ISTInnerNode<V> child = buildIdealISTree(List);
        if (index != 0){
            int key =  List.get(0).key;
            // TODO: change here to write set? or need to do it safely since someone might have done it before me
            newIstTree.keys.set(index -1, key);
        }
        //TODO : this should be done in respect of multi threads later.
        newIstTree.children.set(index,child);
        return true;
    }

    void createIdealCollaborative(int keyCount) {
        if (keyCount < COLLABORATION_THRESHOLD) {
            ArrayList<ISTSingleNode<V>> List = new ArrayList<>();
            List = createKVPairsList(List, oldIstTree,0,keyCount);
            newIstTree = buildIdealISTree(List);
        }
        else {
            newIstTree = new ISTInnerNode<V>((int)Math.floor(Math.sqrt((double) keyCount)));
            //TODO add support in multi threading; if not CAS etc.
        }
       if (keyCount > COLLABORATION_THRESHOLD) {
           while (true) {
               int index = newIstTree.waitQueueIndex ; // TODO: add support - index = READ(newRoot. wait_queue_idx)
               if (index == newIstTree.numOfChildren) break; // each child has a thread working on it;
               newIstTree.waitQueueIndex ++;
               rebuildAndSetChild(keyCount, index); //TODO : add support to multi treading - if CAS(newRoot.wait_queue_idx, index, index + 1) then ...
           }
           for (int i=0; i<newIstTree.numOfChildren; i ++){
               ISTNode<V> child = newIstTree.children.get(i);
               if (child == null) { //can happen only in multi treading- 1 of the threads did not finish
                   if ( ! rebuildAndSetChild(keyCount,i)) {
                       return; //TODO maoras: maybe not needed
                   }
               }
           }
       }

    }

    public int subTreeCount(ISTNode<V> curNode){

        if (curNode instanceof ISTSingleNode){
            return ((ISTSingleNode<V>) curNode).isEmpty ? 0 : 1;
        }
        // here node is inner
        ISTInnerNode<V> innerCurNode = (ISTInnerNode<V>)curNode;
        if (innerCurNode.numOfChildren > COLLABORATION_THRESHOLD) {
            while (true) { // work queue
                int index = innerCurNode.waitQueueIndex++; // TODO: fetch-and-add
                if (index >= innerCurNode.numOfChildren) break;
                subTreeCount(innerCurNode.children.get(index));
            }
        }
        int keyCount = 0;
        for (ISTNode<V> child : innerCurNode.children){
            if (child instanceof ISTSingleNode){
                keyCount += ((ISTSingleNode<V>) child).isEmpty ? 0 : 1;
            } else { // inner
                ISTInnerNode<V> innerChild = (ISTInnerNode<V>)child;
                int count = innerChild.numOfLeaves; // TODO: need to read count & finished atomically!
                boolean finished = innerChild.finishedCount;
                if(finished) {
                    keyCount += count;
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
        int keyCount = subTreeCount(parent);
        createIdealCollaborative(keyCount);
        parent.children.set(index,newIstTree);
        //TODO: add dcss
        return true;
    }
}
