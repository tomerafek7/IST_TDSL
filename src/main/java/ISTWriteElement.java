import java.util.List;

public class ISTWriteElement {
    protected boolean isToInner = false;
    List<ISTNode> childrenList = null;
    protected Integer key = null; // used for Single case (!isToInner)
    protected Object val = null; // used for Single case (!isToInner)
    protected boolean isEmpty = false; // used for Single case (!isToInner)
}
