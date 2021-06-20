import com.sun.tools.javac.util.Pair;

import java.util.ArrayList;

public class IST <V> {

    ISTInnerNode<V> root;
    final static int INIT_SIZE = 1;
    double REBUILD_THRESHOLD = 0.25;
    int MIN_UPDATES_FOR_REBUILD = 10;

    IST(){
        this.root = new ISTInnerNode<>(INIT_SIZE, 0);
        this.root.minKey = Integer.MAX_VALUE;
        this.root.maxKey = Integer.MAX_VALUE;
        //this.root.keys.add(Integer.MAX_VALUE);
        this.root.children.set(0,new ISTSingleNode<>(0, null, true));
        this.root.keys.set(0, Integer.MAX_VALUE);

    }

    public int interpolate(ISTInnerNode<V> node, Integer key){
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

    public V lookup(Integer key){
        ISTNode<V> curNode = root;
        ISTInnerNode<V> parentNode = null;
        // going down the tree
        while (true){
            parentNode = (ISTInnerNode<V>) curNode;
            int idx = interpolate((ISTInnerNode<V>) curNode, key);
            curNode = ((ISTInnerNode<V>) curNode).children.get(idx);
            if(curNode instanceof ISTSingleNode) break;
            // check if rebuild is needed & update curNode if it changed. TODO: maybe check rebuild at the end of the operation
            curNode = checkAndHelpRebuild((ISTInnerNode<V>)curNode, parentNode, idx);
        }
        // reached a leaf
        // TODO: ReadSet(curNode)
        ISTSingleNode<V> leaf = ((ISTSingleNode<V>) curNode);
        if (leaf.key.equals(key) && !leaf.isEmpty){
            return leaf.value;
        } else {
            return null;
        }
    }

    public void insert(Integer key, V value){
        ISTNode<V> curNode = root;
        ISTInnerNode<V> parentNode = null;
        ArrayList<Pair<ISTInnerNode<V>, Integer>> path = new ArrayList<>();
        int idx = -1;
        // going down the tree
        while (true) {
            parentNode = (ISTInnerNode<V>) curNode;
            idx = interpolate((ISTInnerNode<V>) curNode, key);
            path.add(new Pair<>((ISTInnerNode<V>)curNode, idx));
            curNode = ((ISTInnerNode<V>) curNode).children.get(idx);
            if(curNode instanceof ISTSingleNode) break;
            // check if rebuild is needed & update curNode if it changed. TODO: maybe check rebuild at the end of the operation
            curNode = checkAndHelpRebuild((ISTInnerNode<V>)curNode, parentNode, idx);
        }
        // reached a leaf
        // TODO: ReadSet(curNode)
        ISTSingleNode<V> leaf = ((ISTSingleNode<V>) curNode);
        if (!leaf.isEmpty){ // non-empty leaf
            if(leaf.key.equals(key)){ // same key --> just replace value.
                leaf.value = value; //  TODO: Write Set
            } else { // different key --> split into 2 singles
                ArrayList<ISTSingleNode<V>> childrenList = new ArrayList<>();
                ISTSingleNode<V> newSingle = new ISTSingleNode<>(key, value, false);
                // choose the children order: (the first to insert must be the smaller one)
                if(key < leaf.key){
                    childrenList.add(newSingle);
                    childrenList.add((ISTSingleNode<V>)curNode);
                } else {
                    childrenList.add((ISTSingleNode<V>) curNode);
                    childrenList.add(newSingle);
                }
                ISTInnerNode<V> newInner = new ISTInnerNode<>(childrenList,2);
                assert parentNode != null;
                assert idx != -1;
                parentNode.children.set(idx, newInner); // the last index from the loop is our index TODO: Write Set
            }
        } else { // empty leaf
            leaf.key = key;
            leaf.value = value;
            leaf.isEmpty = false;
            // TODO: Write Set
        }

        // increment updates counters
        for (Pair<ISTInnerNode<V>, Integer> pair: path){
            pair.fst.updateCount++;
            // TODO:
            // For node in path:
            // FAA(node.rebuild_counter, 1)
        }

    }

    public void remove(Integer key){
        ISTNode<V> curNode = root;
        ISTInnerNode<V> parentNode = null;
        ArrayList<Pair<ISTInnerNode<V>, Integer>> path = new ArrayList<>();
        int idx = -1;
        // going down the tree
        while (true){
            parentNode = (ISTInnerNode<V>)curNode;
            idx = interpolate((ISTInnerNode<V>) curNode, key);
            path.add(new Pair<>((ISTInnerNode<V>)curNode, idx));
            curNode = ((ISTInnerNode<V>) curNode).children.get(idx);
            if(curNode instanceof ISTSingleNode) break;
            // check if rebuild is needed. TODO: maybe check rebuild at the end of the operation
            curNode = checkAndHelpRebuild((ISTInnerNode<V>)curNode, parentNode, idx);
        }
        // reached a leaf
        // TODO: ReadSet(curNode)
        ISTSingleNode<V> leaf = ((ISTSingleNode<V>) curNode);
        if (!leaf.isEmpty){ // non-empty leaf
            if(leaf.key.equals(key)){ // same key --> DELETE.
                leaf.isEmpty = true; //  TODO: Write Set
            } else { // different key --> ERROR
                // TODO: throw exception?
                assert(false);
            }
        } else { // empty leaf --> ERROR
            // TODO: throw exception?
            assert(false);
        }

        // increment updates counters
        for (Pair<ISTInnerNode<V>, Integer> pair: path){
            pair.fst.updateCount++;
            // TODO:
            // For node in path:
            // FAA(node.rebuild_counter, 1)
        }

    }

    void rebuild(ISTInnerNode<V> rebuildRoot,ISTInnerNode<V> parent, int index){
        rebuildRoot.rebuildObject = new ISTRebuildObject<V>(rebuildRoot,index,parent);
        // TODO: add if needed - result = DCSS(p.children[i], node, op, p.status, [0,⊥,⊥])
        if (rebuildRoot.rebuildObject.helpRebuild()){
        }
        checkRep();
    }

    ISTInnerNode<V> checkAndHelpRebuild(ISTInnerNode<V> root, ISTInnerNode<V> parent, int index){
        if (root.rebuildFlag) {
            root.rebuildObject.helpRebuild();
            return checkAndHelpRebuild((ISTInnerNode<V>) parent.children.get(index), parent, index);
        }
        if (needRebuild(root)) {
            while (true){
                boolean result = true; // = = DCSS(rebuild_flag, 0, 1, active, 0)

                if (result){
                    root.rebuildFlag = true;
                    rebuild(root, parent, index);
                    return checkAndHelpRebuild((ISTInnerNode<V>) parent.children.get(index), parent, index);
                }
                else if (!result){
                    root.rebuildObject.helpRebuild();
                    return checkAndHelpRebuild((ISTInnerNode<V>) parent.children.get(index), parent, index);
                }
            }
        }

        boolean result = !root.rebuildFlag ;//DCSS(active, NA, ++, rebuild_flag, 0)
        if (result) {
            root.activeTX ++;
            return root;
            //FAA(root.active_TX, -1);
        }
        else {
            return checkAndHelpRebuild(root,parent,index);
        }

    }

    boolean needRebuild(ISTInnerNode<V> node){
        return (node.updateCount > (node.numOfLeaves * REBUILD_THRESHOLD) && node.updateCount > MIN_UPDATES_FOR_REBUILD);
    }

    public void print(){
        System.out.println("IST: \n");
        _printIST(root, "");
        System.out.println("\n\n");
    }

    public void _printIST(ISTNode<V> curNode, String indent) {
        if (curNode instanceof ISTSingleNode) {
            String key = ((ISTSingleNode<V>) curNode).isEmpty ? "X" : ((ISTSingleNode<V>) curNode).key.toString();
            System.out.println(indent + key);
        }
        else {
            ISTInnerNode<V> curNodeInner = ((ISTInnerNode<V>) curNode);
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

    public void _checkRep(ISTNode<V> curNode, ArrayList<Pair<Integer, String>> path){

        if (curNode instanceof ISTSingleNode){ // validate path
            Integer key = ((ISTSingleNode<V>) curNode).key;
            for(Pair<Integer, String> pair: path){
                if (pair.snd.equals("right")) assert key >= pair.fst : "key: " + key + ", Path: " + path.toString();
                else if (pair.snd.equals("left")) assert key < pair.fst : "key: " + key + ", Path: " + path.toString();
            }
        } else { // inner node
            ISTInnerNode<V> curNodeInner = (ISTInnerNode<V>) curNode;
            int i=0;
            for(ISTNode<V> child: curNodeInner.children){
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

    public static <V> int debugSubTreeCount(ISTNode<V> curNode){

        if (curNode instanceof ISTSingleNode){
            return ((ISTSingleNode<V>) curNode).isEmpty ? 0 : 1;
        }
        // here node is inner
        ISTInnerNode<V> innerCurNode = (ISTInnerNode<V>)curNode;
        int keyCount = 0;
        for (ISTNode<V> child : innerCurNode.children){
            if (child instanceof ISTSingleNode){
                keyCount += ((ISTSingleNode<V>) child).isEmpty ? 0 : 1;
            } else { // inner
                ISTInnerNode<V> innerChild = (ISTInnerNode<V>)child;
                keyCount += IST.debugSubTreeCount(child);
            }
        }
        innerCurNode.debugNumOfLeaves = keyCount;

        return keyCount;
    }

    public static <V> void debugPrintNumLeaves (ISTNode<V> curNode) {
        IST.debugSubTreeCount(curNode); // update debugNumOfLeaves field for all nodes in subtree
        // now, print only the desired data: (subtree root + sons)
        System.out.println("SubTree root # Leaves is: " + ((ISTInnerNode<V>) curNode).debugNumOfLeaves);
        for (int i = 0; i < ((ISTInnerNode<V>) curNode).numOfChildren; i++) {
            System.out.println("Son #" + i + " # Leaves is: " + ((ISTInnerNode<V>) ((ISTInnerNode<V>) curNode).children.get(i)).debugNumOfLeaves);
        }
    }


}

