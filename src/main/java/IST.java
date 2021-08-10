import com.sun.tools.javac.util.Pair;
import sun.misc.Unsafe;
import java.util.ArrayList;

public class IST {

    ISTNode root;
    final static int INIT_SIZE = 1;
    static final boolean DEBUG_MODE = true;

    IST(){
        this.root = new ISTNode(INIT_SIZE, 0);
        this.root.minKey = Integer.MAX_VALUE;
        this.root.maxKey = Integer.MAX_VALUE;
        //this.root.keys.add(Integer.MAX_VALUE);
        this.root.inner.children.set(0,new ISTNode(0, null, true));
        this.root.inner.keys.set(0, Integer.MAX_VALUE);

    }

    public int interpolate(ISTNode node, Integer key){
        Integer minKey = node.minKey;
        Integer maxKey = node.maxKey;
        int numKeys = node.inner.numOfChildren - 1;

        // check boundaries:
        if (key < minKey) return 0;
        if (key >= maxKey) return numKeys;

        // estimate index:
        long enumerator = numKeys * ((long)key - (long)minKey);
        long denominator = (long)maxKey - (long)minKey;
        int index = (int)(enumerator/denominator);
        Integer indexKey = node.inner.keys.get(index);
        // check estimation:
        // TODO: enhancement: recursive interpolation ?
        if (key < indexKey){
            // going left in the key list
            for(int i = index - 1; i >= 0; i--){
                if (key >= node.inner.keys.get(i)){
                    return i+1; // the +1 is to return the idx in the children list
                }
            }
        } else if (key >= indexKey) {
            // going right in the key list
            for (int i = index + 1; i < numKeys; i++) {
                if (key < node.inner.keys.get(i)) {
                    return i;
                }
            }
        }

        // if we got here it's a bug (because if the key is >max or <min we should exit above)
        assert(false); return -1;

    }

    private ISTNode traverseTree(Integer key, ArrayList<Pair<ISTNode, Integer>> path, LocalStorage localStorage){
        ISTNode curNode = root;
        ISTNode parentNode = null;
        while (true){
            parentNode =  curNode;
            int idx = interpolate( curNode, key);
            path.add(new Pair<>(curNode, idx));
            curNode = (curNode).inner.children.get(idx);
            if(!curNode.isInner){
                // reached a leaf - insert to read-set + check if it's on the write set
                curNode = localStorage.ISTPutIntoReadSet(curNode);
                // if it's still a single , finish
                if(!curNode.isInner) {
                    return curNode;
                }
            }
            // check if rebuild is needed & update curNode if it changed. TODO: maybe check rebuild at the end of the operation
            curNode = checkAndHelpRebuild(curNode, parentNode, idx);
            if(!curNode.isInner) { // corner case of a rebuild with all empty leaves
                return curNode;
            }
        }
    }

    public Object lookup(Integer key){
        LocalStorage localStorage = TX.lStorage.get();
        ArrayList<Pair<ISTNode, Integer>> path = new ArrayList<>();

        // traverse the tree
        ISTNode leaf = traverseTree(key, path, localStorage);
        if (!leaf.single.isEmpty && leaf.single.key.equals(key)){
            return leaf.single.value;
        } else {
            return null;
        }
    }

    public void insert(Integer key, Object value){
        LocalStorage localStorage = TX.lStorage.get();

        ArrayList<Pair<ISTNode, Integer>> path = new ArrayList<>();

        // traverse the tree
        ISTNode leaf = (traverseTree(key, path, localStorage));
        // get parent + idx from the last element in path (path is updated in traverseTree)
        ISTNode parentNode = path.get(path.size()-1).fst;
        Integer idx = path.get(path.size()-1).snd;
        if (!leaf.single.isEmpty){ // non-empty leaf
            if(leaf.single.key.equals(key)){ // same key --> just replace value.
                localStorage.ISTPutIntoWriteSet(leaf, false,null,key,value,false);
            } else { // different key --> split into 2 singles
                ArrayList<ISTNode> childrenList = new ArrayList<>();
                ISTNode newSingle1 = new ISTNode(leaf.single.key, leaf.single.value, false);
                ISTNode newSingle2 = new ISTNode(key, value, false);
                // choose the children order: (the first to insert must be the smaller one)
                if (newSingle1.single.key < newSingle2.single.key) {
                    childrenList.add(newSingle1);
                    childrenList.add(newSingle2);
                } else {
                    childrenList.add(newSingle2);
                    childrenList.add(newSingle1);
                }
                assert parentNode != null;
                assert idx != -1;
                localStorage.ISTPutIntoWriteSet(leaf,true, childrenList,null,null,true);
            }
        } else { // empty leaf
            localStorage.ISTPutIntoWriteSet(leaf,false, null, key, value, false);

        }

        // increment updates counters
        for (Pair<ISTNode, Integer> pair: path) {
            localStorage.incUpdateList.add(pair.fst);
        }

    }

    public void remove(Integer key){
        LocalStorage localStorage = TX.lStorage.get();

        ArrayList<Pair<ISTNode, Integer>> path = new ArrayList<>();

        // traverse the tree
        ISTNode leaf = (traverseTree(key, path, localStorage));
        if (!leaf.single.isEmpty){ // non-empty leaf
            if(leaf.single.key.equals(key)){ // same key --> DELETE.
                //leaf.isEmpty = true;
                localStorage.ISTPutIntoWriteSet(leaf, false, null,null,null,true);
            } else { // different key --> ERROR
                // TODO: throw exception?
                assert(false);
            }
        } else { // empty leaf --> ERROR
            // TODO: throw exception?
            assert(false);
        }

        // increment updates counters
        for (Pair<ISTNode, Integer> pair: path) {
            localStorage.incUpdateList.add(pair.fst);
        }
    }

    void rebuild(ISTNode rebuildRoot,ISTNode parent, int index){
        // try to change the atomic reference to rebuild object:
        rebuildRoot.inner.rebuildObjectAtomicReference.compareAndSet(null, new ISTRebuildObject(rebuildRoot,index,parent));
        rebuildRoot.inner.rebuildObject = rebuildRoot.inner.rebuildObjectAtomicReference.get(); // at this point someone must have changed the reference.
        // TODO: add if needed - result = DCSS(p.children[i], node, op, p.status, [0,⊥,⊥])
        if (rebuildRoot.inner.rebuildObject.helpRebuild()){
        }
        checkRep();
    }

    ISTNode checkAndHelpRebuild(ISTNode root, ISTNode parent, int index){
        LocalStorage localStorage = TX.lStorage.get();
        //if(DEBUG_MODE) System.out.println("Thread ID = " + localStorage.tid);
        if(!root.isInner){
            return root; // corner case - rebuild is done with 0 leaves (all are empty) - so the new root is single
        }
//        if(root.inner.activeThreadsSet.contains(localStorage.tid)){ // free pass - in case this TX is already inside this node
//            if(root.inner.activeTX.get() == -1){
//                int x = 1;
//            }
//            assert (root.inner.activeTX.get() != -1) : "ERROR: (TID = " + localStorage.tid + ") ActiveTX shouldn't be = -1. threadSet = " + root.inner.activeThreadsSet.toString();
//            // no one else cannot start rebuild because this thread is inside the node, just to make sure..
//            return root;
//        }
        if (root.inner.activeTX.get() == -1) { // if (rebuild_flag)
            //root.inner.rebuildObject.helpRebuild();
            rebuild(root, parent, index); // to get the rebuild object (or create it in case it's null), and then call help rebuild
            assert(parent.inner.children.get(index).inner.activeTX.get() != -1) : "ERROR: AFTER REBUILD: (TID = " + localStorage.tid + ") ActiveTX shouldn't be = -1";
            return checkAndHelpRebuild( parent.inner.children.get(index), parent, index); // call again, to make sure we hold the updated sub-tree root
        }
        if (root.inner.needRebuildVersion >= 0L && localStorage.TxNum > root.inner.needRebuildVersion) { // enter this only if rebuild is needed AND this TX is younger than when rebuild was needed (to prevent deadlocks)
            boolean result = false;
            while (root.inner.activeTX.get() != -1) {// we wait for all transactions in sub-tree to be over and than rebuild should catch the sub-tree
                if(DEBUG_MODE) System.out.println("Waiting for ActiveTX to be 0. activeTX = " + root.inner.activeTX.get());
                result = root.inner.activeTX.compareAndSet(0, -1);
            }
                if (result){
                    rebuild(root, parent, index); // to get the rebuild object (or create it in case it's null), and then call help rebuild
                    assert(parent.inner.children.get(index).inner.activeTX.get() != -1) : "ERROR: AFTER REBUILD: (TID = " + localStorage.tid + ") ActiveTX shouldn't be = -1";
                    return checkAndHelpRebuild(parent.inner.children.get(index), parent, index); // call again, to make sure we hold the updated sub-tree root
                }
                else {
                    return checkAndHelpRebuild(parent.inner.children.get(index), parent, index); // call again, to make sure we hold the updated sub-tree root
                }
        }

        while(true){
            int active = root.inner.activeTX.get();
            boolean result = false;
            if(active == -1){ // rebuild started, go help
                return checkAndHelpRebuild(root,parent,index);
            } else{ // try to inc the active counter
                result = root.inner.activeTX.compareAndSet(active, active+1);
                if(result) {
                    localStorage.decActiveList.add(root);
                    // root.inner.activeThreadsSet.add(localStorage.tid); // adding the TID to the set so we could know that this thread has a free pass
                    return root; // return the updated sub-tree root, so the operation will continue with the updated tree // SUCCESS
                }
            }
        }

    }

//    boolean needRebuild(ISTNode node){
//        return (node.inner.updateCount > (node.inner.numOfLeaves * REBUILD_THRESHOLD) && node.inner.updateCount > MIN_UPDATES_FOR_REBUILD);
//    }

    public void print(){
        System.out.println("IST: \n");
        _printIST(root, "");
        System.out.println("\n\n");
    }

    public void _printIST(ISTNode curNode, String indent) {
        if (!curNode.isInner) {
            String key = curNode.single.isEmpty ? "X" : curNode.single.key.toString();
            System.out.println(indent + key);
        }
        else {
            ISTNode curNodeInner = curNode;
            for(int i=curNodeInner.inner.numOfChildren-1; i>=0; i--){
                if(i < curNodeInner.inner.numOfChildren-1){
                    System.out.println(indent + curNodeInner.inner.keys.get(i));
                }
                _printIST(curNodeInner.inner.children.get(i), indent + "      ");
            }
        }
    }

    // check that the tree structure is correct
    public void checkRep(){
        _checkRep(root, new ArrayList<>());
    }

    public void _checkRep(ISTNode curNode, ArrayList<Pair<Integer, String>> path){

        if (!curNode.isInner){ // validate path
            if(!curNode.single.isEmpty) {
                Integer key = curNode.single.key;
                for (Pair<Integer, String> pair : path) {
                    if (pair.snd.equals("right")) assert key >= pair.fst : "key: " + key + ", Path: " + path.toString();
                    else if (pair.snd.equals("left"))
                        assert key < pair.fst : "key: " + key + ", Path: " + path.toString();
                }
            }
        } else { // inner node
            int i=0;
            for(ISTNode child: curNode.inner.children){
                ArrayList<Pair<Integer, String>> newPath = new ArrayList<>(path);
                if (i==0){
                    newPath.add(new Pair<>(curNode.inner.keys.get(i), "left"));
                } else{
                    newPath.add(new Pair<>(curNode.inner.keys.get(i-1), "right"));
                }
                if(child == null){
                    int x = 1;
                }
                _checkRep(child, newPath);
                i++;
            }
        }
    }

    public static  int debugSubTreeCount(ISTNode curNode){

        if (!curNode.isInner){
            return curNode.single.isEmpty ? 0 : 1;
        }
        // here node is inner
        int keyCount = 0;
        for (ISTNode child : curNode.inner.children){
            if (!child.isInner){
                keyCount +=  child.single.isEmpty ? 0 : 1;
            } else { // inner
                keyCount += IST.debugSubTreeCount(child);
            }
        }
        curNode.inner.debugNumOfLeaves = keyCount;

        return keyCount;
    }

    public static  void debugPrintNumLeaves (ISTNode curNode) {
        IST.debugSubTreeCount(curNode); // update debugNumOfLeaves field for all nodes in subtree
        // now, print only the desired data: (subtree root + sons)
        System.out.println("SubTree root # Leaves is: " + curNode.inner.debugNumOfLeaves);
        for (int i = 0; i <  curNode.inner.numOfChildren; i++) {
            System.out.println("Son #" + i + " # Leaves is: " + curNode.inner.children.get(i).inner.debugNumOfLeaves);
        }
    }


}

