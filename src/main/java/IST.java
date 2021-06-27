import com.sun.tools.javac.util.Pair;
import sun.misc.Unsafe;
import java.util.ArrayList;

public class IST {

    ISTInnerNode root;
    final static int INIT_SIZE = 1;
    double REBUILD_THRESHOLD = 0.25;
    int MIN_UPDATES_FOR_REBUILD = 10;
    static final boolean DEBUG_MODE = true;

    IST(){
        this.root = new ISTInnerNode(INIT_SIZE, 0);
        this.root.minKey = Integer.MAX_VALUE;
        this.root.maxKey = Integer.MAX_VALUE;
        //this.root.keys.add(Integer.MAX_VALUE);
        this.root.children.set(0,new ISTSingleNode(0, null, true));
        this.root.keys.set(0, Integer.MAX_VALUE);

    }

    public int interpolate(ISTInnerNode node, Integer key){
        Integer minKey = node.minKey;
        Integer maxKey = node.maxKey;
        int numKeys = node.numOfChildren - 1;

        // check boundaries:
        if (key < minKey) return 0;
        if (key >= maxKey) return numKeys;

        // estimate index:
        int index = (numKeys * (key - minKey) / (maxKey - minKey));
        Integer indexKey = node.keys.get(index);
        // check estimation:
        // TODO: enhancement: recursive interpolation ?
        if (key < indexKey){
            // going left in the key list
            for(int i = index - 1; i >= 0; i--){
                if (key >= node.keys.get(i)){
                    return i+1; // the +1 is to return the idx in the children list
                }
            }
        } else if (key >= indexKey) {
            // going right in the key list
            for (int i = index + 1; i < numKeys; i++) {
                if (key < node.keys.get(i)) {
                    return i;
                }
            }
        }

        // if we got here it's a bug (because if the key is >max or <min we should exit above)
        assert(false); return -1;

    }

    private ISTNode traverseTree(Integer key, ArrayList<Pair<ISTInnerNode, Integer>> path, LocalStorage localStorage){
        ISTNode curNode = root;
        ISTInnerNode parentNode = null;
        while (true){
            parentNode = (ISTInnerNode) curNode;
            int idx = interpolate((ISTInnerNode) curNode, key);
            path.add(new Pair<>((ISTInnerNode)curNode, idx));
            curNode = ((ISTInnerNode) curNode).children.get(idx);
            if(curNode instanceof ISTSingleNode){
                // reached a leaf - insert to read-set + check if it's on the write set
                curNode = localStorage.ISTPutIntoReadSet(curNode, parentNode, idx);
                // if it's still a single , finish
                if(curNode instanceof ISTSingleNode) {
                    return curNode;
                }
            }
            // check if rebuild is needed & update curNode if it changed. TODO: maybe check rebuild at the end of the operation
            curNode = checkAndHelpRebuild((ISTInnerNode)curNode, parentNode, idx);
        }
    }

    public Object lookup(Integer key){
        LocalStorage localStorage = TX.lStorage.get();
        ArrayList<Pair<ISTInnerNode, Integer>> path = new ArrayList<>();
//        ISTNode curNode = root;
//        ISTInnerNode parentNode = null;
//        // going down the tree
//        while (true){
//            parentNode = (ISTInnerNode) curNode;
//            int idx = interpolate((ISTInnerNode) curNode, key);
//            curNode = ((ISTInnerNode) curNode).children.get(idx);
//            if(curNode instanceof ISTSingleNode){
//                // reached a leaf - insert to read-set + check if it's on the write set
//                curNode = localStorage.ISTPutIntoReadSet(curNode, parentNode, idx);
//                // if it's still a single , break
//                if(curNode instanceof ISTSingleNode) {
//                    break;
//                }
//            }
//            // check if rebuild is needed & update curNode if it changed. TODO: maybe check rebuild at the end of the operation
//            curNode = checkAndHelpRebuild((ISTInnerNode)curNode, parentNode, idx);
//        }
        // traverse the tree
        ISTSingleNode leaf = ((ISTSingleNode) traverseTree(key, path, localStorage));
        if (!leaf.isEmpty && leaf.key.equals(key)){
            return leaf.value;
        } else {
            return null;
        }
    }

    public void insert(Integer key, Object value){
        LocalStorage localStorage = TX.lStorage.get();
//        ISTNode curNode = root;
//        ISTInnerNode parentNode = null;
        ArrayList<Pair<ISTInnerNode, Integer>> path = new ArrayList<>();
//        int idx = -1;
//        // going down the tree
//        while (true) {
//            parentNode = (ISTInnerNode) curNode;
//            idx = interpolate((ISTInnerNode) curNode, key);
//            path.add(new Pair<>((ISTInnerNode)curNode, idx));
//            curNode = ((ISTInnerNode) curNode).children.get(idx);
//            if(curNode instanceof ISTSingleNode) break;
//            // check if rebuild is needed & update curNode if it changed. TODO: maybe check rebuild at the end of the operation
//            curNode = checkAndHelpRebuild((ISTInnerNode)curNode, parentNode, idx);
//        }
        // traverse the tree
        ISTSingleNode leaf = ((ISTSingleNode) traverseTree(key, path, localStorage));
        // get parent + idx from the last element in path (path is updated in traverseTree)
        ISTInnerNode parentNode = path.get(path.size()-1).fst;
        Integer idx = path.get(path.size()-1).snd;
        if (!leaf.isEmpty){ // non-empty leaf
            if(leaf.key.equals(key)){ // same key --> just replace value.
                //leaf.value = value;
                localStorage.ISTPutIntoSingleWriteSet(leaf, key, value, false);
            } else { // different key --> split into 2 singles
                ArrayList<ISTSingleNode> childrenList = new ArrayList<>();
                ISTSingleNode newSingle = new ISTSingleNode(key, value, false);
                // choose the children order: (the first to insert must be the smaller one)
                if (key < leaf.key) {
                    childrenList.add(newSingle);
                    childrenList.add(leaf);
                } else {
                    childrenList.add(leaf);
                    childrenList.add(newSingle);
                }
                ISTInnerNode newInner = new ISTInnerNode(childrenList, 2);
                assert parentNode != null;
                assert idx != -1;
                //parentNode.children.set(idx, newInner); // the last index from the loop is our index
                localStorage.ISTPutIntoInnerWriteSet(parentNode, idx, newInner);
                // add to write set the old son (curNode), to force invalidation : (do not care about the values)
                localStorage.ISTPutIntoSingleWriteSet(leaf, null, null, true);
            }
        } else { // empty leaf
//            leaf.key = key;
//            leaf.value = value;
//            leaf.isEmpty = false;
            localStorage.ISTPutIntoSingleWriteSet(leaf, key, value, false);

        }

        // increment updates counters
        for (Pair<ISTInnerNode, Integer> pair: path) {
            localStorage.incUpdateList.add(pair.fst);
        }

    }

    public void remove(Integer key){
        LocalStorage localStorage = TX.lStorage.get();
//        ISTNode curNode = root;
//        ISTInnerNode parentNode = null;
        ArrayList<Pair<ISTInnerNode, Integer>> path = new ArrayList<>();
//        int idx = -1;
//        // going down the tree
//        while (true){
//            parentNode = (ISTInnerNode)curNode;
//            idx = interpolate((ISTInnerNode) curNode, key);
//            path.add(new Pair<>((ISTInnerNode)curNode, idx));
//            curNode = ((ISTInnerNode) curNode).children.get(idx);
//            if(curNode instanceof ISTSingleNode) break;
//            // check if rebuild is needed. TODO: maybe check rebuild at the end of the operation
//            curNode = checkAndHelpRebuild((ISTInnerNode)curNode, parentNode, idx);
//        }
//        ISTSingleNode leaf = ((ISTSingleNode) curNode);
        // traverse the tree
        ISTSingleNode leaf = ((ISTSingleNode) traverseTree(key, path, localStorage));
        // get parent + idx from the last element in path (path is updated in traverseTree)
//        ISTInnerNode parentNode = path.get(path.size()-1).fst;
//        Integer idx = path.get(path.size()-1).snd;
        if (!leaf.isEmpty){ // non-empty leaf
            if(leaf.key.equals(key)){ // same key --> DELETE.
                //leaf.isEmpty = true;
                localStorage.ISTPutIntoSingleWriteSet(leaf, null, null, true);
            } else { // different key --> ERROR
                // TODO: throw exception?
                assert(false);
            }
        } else { // empty leaf --> ERROR
            // TODO: throw exception?
            assert(false);
        }

        // increment updates counters
        for (Pair<ISTInnerNode, Integer> pair: path) {
            localStorage.incUpdateList.add(pair.fst);
        }
    }

    void rebuild(ISTInnerNode rebuildRoot,ISTInnerNode parent, int index){
        rebuildRoot.rebuildObject = new ISTRebuildObject(rebuildRoot,index,parent);
        // TODO: add if needed - result = DCSS(p.children[i], node, op, p.status, [0,⊥,⊥])
        if (rebuildRoot.rebuildObject.helpRebuild()){
        }
        checkRep();
    }

    ISTInnerNode checkAndHelpRebuild(ISTInnerNode root, ISTInnerNode parent, int index){
        LocalStorage localStorage = TX.lStorage.get();
        if (root.activeTX.get() == -1) { // if (rebuild_flag)

           // root.rebuildObject.helpRebuild();
            return checkAndHelpRebuild((ISTInnerNode) parent.children.get(index), parent, index); // call again, to make sure we hold the updated sub-tree root
        }
        if (needRebuild(root)) {
            boolean result = false;
            while (root.activeTX.get() != -1) {// we wait for all transactions in sub-tree to be over and than reubild should catch the sub-tree
                result = root.activeTX.compareAndSet(0, -1);
            }
                if (result){
                    rebuild(root, parent, index); // we "catched" the rebuild first.
                    return checkAndHelpRebuild((ISTInnerNode) parent.children.get(index), parent, index); // call again, to make sure we hold the updated sub-tree root
                }
                else {
                    //root.rebuildObject.helpRebuild();
                    return checkAndHelpRebuild((ISTInnerNode) parent.children.get(index), parent, index); // call again, to make sure we hold the updated sub-tree root
                }

        }

        while(true){
            int active = root.activeTX.get();
            boolean result = false;
            if(active == -1){ // rebuild started, go help
                return checkAndHelpRebuild(root,parent,index);
            } else{ // try to inc the active counter
                result = root.activeTX.compareAndSet(active, active+1);
                localStorage.decActiveList.add(root);
                if (result) return root; // return the updated sub-tree root, so the operation will continue with the updated tree // SUCCESS
            }
        }

    }

    boolean needRebuild(ISTInnerNode node){
        return (node.updateCount > (node.numOfLeaves * REBUILD_THRESHOLD) && node.updateCount > MIN_UPDATES_FOR_REBUILD);
    }

    public void print(){
        System.out.println("IST: \n");
        _printIST(root, "");
        System.out.println("\n\n");
    }

    public void _printIST(ISTNode curNode, String indent) {
        if (curNode instanceof ISTSingleNode) {
            String key = ((ISTSingleNode) curNode).isEmpty ? "X" : ((ISTSingleNode) curNode).key.toString();
            System.out.println(indent + key);
        }
        else {
            ISTInnerNode curNodeInner = ((ISTInnerNode) curNode);
            for(int i=curNodeInner.numOfChildren-1; i>=0; i--){
                if(i < curNodeInner.numOfChildren-1){
                    System.out.println(indent + curNodeInner.keys.get(i));
                }
                _printIST(curNodeInner.children.get(i), indent + "      ");
            }
        }
    }

    // check that the tree structure is correct
    public void checkRep(){
        _checkRep(root, new ArrayList<>());
    }

    public void _checkRep(ISTNode curNode, ArrayList<Pair<Integer, String>> path){

        if (curNode instanceof ISTSingleNode){ // validate path
            Integer key = ((ISTSingleNode) curNode).key;
            for(Pair<Integer, String> pair: path){
                if (pair.snd.equals("right")) assert key >= pair.fst : "key: " + key + ", Path: " + path.toString();
                else if (pair.snd.equals("left")) assert key < pair.fst : "key: " + key + ", Path: " + path.toString();
            }
        } else { // inner node
            ISTInnerNode curNodeInner = (ISTInnerNode) curNode;
            int i=0;
            for(ISTNode child: curNodeInner.children){
                ArrayList<Pair<Integer, String>> newPath = new ArrayList<>(path);
                if (i==0){
                    newPath.add(new Pair<>(curNodeInner.keys.get(i), "left"));
                } else{
                    newPath.add(new Pair<>(curNodeInner.keys.get(i-1), "right"));
                }
                _checkRep(child, newPath);
                i++;
            }
        }
    }

    public static  int debugSubTreeCount(ISTNode curNode){

        if (curNode instanceof ISTSingleNode){
            return ((ISTSingleNode) curNode).isEmpty ? 0 : 1;
        }
        // here node is inner
        ISTInnerNode innerCurNode = (ISTInnerNode)curNode;
        int keyCount = 0;
        for (ISTNode child : innerCurNode.children){
            if (child instanceof ISTSingleNode){
                keyCount += ((ISTSingleNode) child).isEmpty ? 0 : 1;
            } else { // inner
                ISTInnerNode innerChild = (ISTInnerNode)child;
                keyCount += IST.debugSubTreeCount(child);
            }
        }
        innerCurNode.debugNumOfLeaves = keyCount;

        return keyCount;
    }

    public static  void debugPrintNumLeaves (ISTNode curNode) {
        IST.debugSubTreeCount(curNode); // update debugNumOfLeaves field for all nodes in subtree
        // now, print only the desired data: (subtree root + sons)
        System.out.println("SubTree root # Leaves is: " + ((ISTInnerNode) curNode).debugNumOfLeaves);
        for (int i = 0; i < ((ISTInnerNode) curNode).numOfChildren; i++) {
            System.out.println("Son #" + i + " # Leaves is: " + ((ISTInnerNode) ((ISTInnerNode) curNode).children.get(i)).debugNumOfLeaves);
        }
    }


}

