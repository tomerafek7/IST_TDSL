public class ISTSingleNode extends ISTNode{
    Integer key;
    Object value;
    boolean isEmpty;

    ISTSingleNode(Integer key, Object value, boolean isEmpty){
       this.key = key;
       this.value = value;
       this.isEmpty = isEmpty;
       this.minKey = key;
       this.maxKey = key;
    }

    @Override
    public String toString() {
        return key.toString();
    }
}



