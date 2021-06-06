import java.util.ArrayList;

public class ISTRebuildObject<V> {
    IST<V> oldIstTree;
    IST<V> newIstTree;
    static final int MIN_TREE_LEAF_SIZE = 4; //TODO: might fight a better value


    public ISTNode buildIdealISTree (ArrayList<ISTSingleNode<V>> KVPairList){
        if (KVPairList.size()< MIN_TREE_LEAF_SIZE){
            ISTInnerNode<V> myNode = new ISTInnerNode<V>(KVPairList);
            myNode.children = new ArrayList<ISTNode<V>>();
            myNode.children.add(KVPairList.get(0));
        }
    }
}
