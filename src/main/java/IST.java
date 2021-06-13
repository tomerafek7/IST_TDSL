import com.sun.tools.javac.util.Pair;
import org.junit.function.ThrowingRunnable;

import java.util.ArrayList;

public class IST <V> {

    ISTInnerNode<V> root;
    final static int INIT_SIZE = 1;
    double REBUILD_THRESHOLD = 0.25;
    int MIN_UPDATES_FOR_REBUILD = 10;

    IST(){
        this.root = new ISTInnerNode<>(INIT_SIZE);
        this.root.minKey = Integer.MAX_VALUE;
        this.root.maxKey = Integer.MAX_VALUE;
        //this.root.keys.add(Integer.MAX_VALUE);
        this.root.children.add(new ISTSingleNode<>(0, null, true));

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
            // going right in the key list
            for(int i=index; i>0; i--){
                if (key >= node.keys.get(i)){
                    return i+1; // the +1 is to return the idx in the children list
                }
            }
        } else if (key >= indexKey) {
            // going left in the key list
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
        ArrayList<Pair<ISTNode<V>, Integer>> path = new ArrayList<>(); // TODO: not sure if needed
        // going down the tree
        while (!(curNode instanceof ISTSingleNode)){
            // TODO: checkAndHelpRebuild
            int idx = interpolate((ISTInnerNode<V>) curNode, key);
            path.add(new Pair<>(curNode, idx));  // TODO: not sure if needed
            curNode = ((ISTInnerNode<V>) curNode).children.get(idx);
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
        ArrayList<Pair<ISTNode<V>, Integer>> path = new ArrayList<>();
        int idx = -1;
        // going down the tree
        while (!(curNode instanceof ISTSingleNode)){
            parentNode = (ISTInnerNode<V>)curNode;
            // TODO: checkAndHelpRebuild
            idx = interpolate((ISTInnerNode<V>) curNode, key);
            path.add(new Pair<>(curNode, idx));
            curNode = ((ISTInnerNode<V>) curNode).children.get(idx);
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
                ISTInnerNode<V> newInner = new ISTInnerNode<>(childrenList);
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

        // TODO:
        // For node in path:
        // FAA(node.rebuild_counter, 1)

    }

    public void remove(Integer key){
        ISTNode<V> curNode = root;
        ISTInnerNode<V> parentNode = null;
        ArrayList<Pair<ISTNode<V>, Integer>> path = new ArrayList<>();
        int idx = -1;
        // going down the tree
        while (!(curNode instanceof ISTSingleNode)){
            parentNode = (ISTInnerNode<V>)curNode;
            // TODO: checkAndHelpRebuild
            idx = interpolate((ISTInnerNode<V>) curNode, key);
            path.add(new Pair<>(curNode, idx));
            curNode = ((ISTInnerNode<V>) curNode).children.get(idx);
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

        // TODO:
        // For node in path:
        // FAA(node.rebuild_counter, 1)
    }

    void rebuild(ISTInnerNode<V> rebuildRoot,ISTInnerNode<V> parent, int index){
        rebuildRoot.rebuildObject = new ISTRebuildObject<V>(rebuildRoot,index,parent);
        // TODO: add if needed - result = DCSS(p.children[i], node, op, p.status, [0,⊥,⊥])
        if (rebuildRoot.rebuildObject.helpRebuild()){

        }
    }
    void checkAndHelpRebuild(ISTInnerNode<V> root, ISTInnerNode<V> parent, int index){
        if (root.rebuildFlag) {
            root.rebuildObject.helpRebuild();
        }
        if (needRebuild(root)) {
            while (true){
                boolean result = root.activeTX == 0; // = = DCSS(rebuild_flag, 0, 1, active, 0)

                if (result){
                    root.rebuildFlag = true;
                    rebuild(root, parent, index);
                }
                else if (!result){
                    root.rebuildObject.helpRebuild();
                    break;
                }
            }
        }

    }
    boolean needRebuild(ISTInnerNode<V> root){
        return (root.updateCount > (root.numOfLeaves * REBUILD_THRESHOLD) && root.updateCount > MIN_UPDATES_FOR_REBUILD);
    }

}
