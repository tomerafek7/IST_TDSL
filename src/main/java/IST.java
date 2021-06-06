import java.util.Collections;

public class IST <V> {

    ISTNode<V> root;

    public int interpolate(ISTInnerNode<V> node, Integer key){
        Integer minKey = Collections.min(node.keys);
        Integer maxKey = Collections.max(node.keys);
        int numKeys = node.numOfChildren - 1;

        // check boundaries:
        if (key.compareTo(minKey) < 0) return 0;
        else if (key.compareTo(maxKey) >= 0) return numKeys;

        // estimate index:
        int index = (numKeys * (key - minKey) / (maxKey - minKey));

    }

    public V lookup(Integer key){

        ISTNode<V> curNode = root;
        while (!(curNode instanceof ISTSingleNode)){
            // checkAndHelpRebuild - TODO
            int idx = interpolate((ISTInnerNode<V>) curNode, key);
        }


    }


}
