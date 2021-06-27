public class ISTSingleNode {
    Integer key;
    Object value;
    boolean isEmpty;

    ISTSingleNode(Integer key, Object value, boolean isEmpty){
       this.key = key;
       this.value = value;
       this.isEmpty = isEmpty;
    }

    @Override
    public String toString() {
        return key.toString();
    }
}



