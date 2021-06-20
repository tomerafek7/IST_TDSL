public class ISTWriteElement {
    protected boolean isLeaf = false;
    protected ISTNode son = null; // used for Inner case (isLeaf = false)
    protected Integer index = null; // used for Inner case (isLeaf = false)
    protected Integer key = null; // used for Single case (isLeaf = true) - insert
    protected Object val = null; // used for Single case (isLeaf = true) - insert
    protected boolean isEmpty = false; // used for Single case (isLeaf = true) - delete
}
