import org.junit.Assert;

import java.util.List;

public class ISTComplexRun implements Runnable {

    String name;
    IST tree;
    List<Integer> keyList;
    List<Integer> valueList;
    String operation;
    boolean validOperation;

    public ISTComplexRun(String name, IST tree, List<Integer> keyList, List<Integer> valueList, String operation,boolean validOperation){
        this.name = name;
        this.tree = tree;
        this.keyList = keyList;
        this.valueList = valueList;
        this.operation = operation;
        this.validOperation = validOperation;
    }

    @Override
    public void run() {
        while (true) {
            try {
                TX.TXbegin();
                for (int i = 0; i < keyList.size(); i++) {
                    if (operation.equals("insert")) {
                        tree.insert(keyList.get(i), valueList.get(i));
                    } else if (operation.equals("remove")) {
                        if (validOperation) {
                            tree.remove(keyList.get(i));
                        } else {
                            int finalI = i;
                            Assert.assertThrows(AssertionError.class, () -> tree.remove(keyList.get(finalI)));
                        }


                    } else {
                        if (validOperation) {
                            Assert.assertEquals(valueList.get(i), tree.lookup(keyList.get(i)));
                        } else {
                            Assert.assertNull(tree.lookup(keyList.get(i)));
                        }

                    }
                }
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
            } finally {
                TX.TXend();
            }
            break;
        }
    }
}