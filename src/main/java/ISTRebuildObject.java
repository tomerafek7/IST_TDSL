import java.util.ArrayList;
import java.lang.Math;
import java.util.List;

public class ISTRebuildObject {
    ISTInnerNode oldIstTree;
    ISTInnerNode newIstTree;
    int index;//TODO: check if needed
    ISTInnerNode parent;
    boolean finishedRebuild;

    ISTRebuildObject(ISTInnerNode oldTree, int indexInParent, ISTInnerNode parentNode){
        oldIstTree = oldTree;
        index = indexInParent;
        parent = parentNode;
        finishedRebuild = false;

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

    boolean rebuildAndSetChild (int keyCount,int index) { //keyCount is of the parent node which initiated the rebuild
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
            // TODO: change here to write set? or need to do it safely since someone might have done it before me
            newIstTree.keys.set(index -1, key);
            if (index == 1) {
                newIstTree.minKey = key;
            } else if(index == totalChildren -1){
                newIstTree.maxKey = key;
            }
        }
        //TODO : this should be done in respect of multi threads later.
        newIstTree.children.set(index,child);////////////////
        return true;
    }

    void createIdealCollaborative(int keyCount) {
        if (keyCount < COLLABORATION_THRESHOLD) {
            ArrayList<ISTSingleNode> list = new ArrayList<>();
            list = createKVPairsList(list, oldIstTree,0, keyCount);
            newIstTree = buildIdealISTree(list);
        }
        else {
            newIstTree = new ISTInnerNode((int)Math.floor(Math.sqrt((double) keyCount)), keyCount);
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
               ISTNode child = newIstTree.children.get(i);
               if (child == null) { //can happen only in multi treading- 1 of the threads did not finish
                   if ( ! rebuildAndSetChild(keyCount,i)) {
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
                int index = innerCurNode.waitQueueIndex++; // TODO: fetch-and-add
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
        if (finishedRebuild){
            return false; // in this case we need to update the root (outside)
        }
        int keyCount = subTreeCount(oldIstTree);
        createIdealCollaborative(keyCount);
        parent.children.set(index,newIstTree); // DCSS(p.children[op.index], op, ideal, p.status, [0,⊥,⊥])
        //TODO: add dcss with finished rebuild
        // TODO: is CAS enough here?
        return true;
    }
}
