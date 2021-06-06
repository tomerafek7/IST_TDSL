import java.util.ArrayList;
import java.lang.Math;

public class ISTRebuildObject<V> {
    IST<V> oldIstTree;
    IST<V> newIstTree;
    static final int MIN_TREE_LEAF_SIZE = 4; //TODO: might fight a better value


    public ISTNode<V> buildIdealISTree (ArrayList<ISTSingleNode<V>> KVPairList){
        int numOfLeaves = KVPairList.size();
        if (numOfLeaves< MIN_TREE_LEAF_SIZE){
            ISTInnerNode<V> myNode = new ISTInnerNode<V>(KVPairList);
            myNode.children = new ArrayList<ISTNode<V>>();
            myNode.children.add(KVPairList.get(0));
            return myNode;
        }
        int numChildren = (int) Math.ceil(Math.sqrt(numOfLeaves));
        int childSize = (int) Math.floor((((double)numOfLeaves)/numChildren));
        int remainder = numOfLeaves%numChildren;
        ISTInnerNode<V> myNode = new ISTInnerNode<V>(numChildren);
        for (int i = 0; i<numChildren; i++){
            int size = childSize + (i<remainder ? 1 : 0);
            myNode.children.add(buildIdealISTree((ArrayList<ISTSingleNode<V>>) KVPairList.subList(0,size-1)));
            if (i != 0){
                myNode.keys.add(KVPairList.get(0).key);

            }
            KVPairList = (ArrayList<ISTSingleNode<V>>)KVPairList.subList(size,KVPairList.size()-1);
        }
        return myNode;

    }
}
