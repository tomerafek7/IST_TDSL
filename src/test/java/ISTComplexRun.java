import org.junit.Assert;

import java.util.List;

public class ISTComplexRun implements Runnable {

    String name;
    IST tree;
    List<Integer> keyList;
    List<Integer> valueList;
    String operation;
    boolean validOperation;
    static double INSERT_PERCENT = 0.5;

    public ISTComplexRun(String name, IST tree, List<Integer> keyList, List<Integer> valueList, String operation, boolean validOperation){
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
                try {
                    TX.TXbegin();
                    for (int i = 0; i < keyList.size(); i++) {
                        String op = operation;
                        if(operation.equals("mixed")){
                            if(i % 10 < 10*INSERT_PERCENT) op = "insert";
                            else op = "remove";
                        }
                        if (op.equals("insert")) {
                            System.out.println("insert: " + name + ", key: " + keyList.get(i));
                            tree.insert(keyList.get(i), valueList.get(i));
                        } else if (op.equals("remove")) {
                            System.out.println("remove: " + name);
                            if (validOperation) {
                                tree.remove(keyList.get(i));
                            } else {
                                int finalI = i;
                                Assert.assertThrows(AssertionError.class, () -> tree.remove(keyList.get(finalI)));
                            }
                        } else { // lookup
                            System.out.println("lookup: " + name);
                            if (validOperation) {
                                Assert.assertEquals(valueList.get(i), tree.lookup(keyList.get(i)));
                            } else {
                                Assert.assertNull(tree.lookup(keyList.get(i)));
                            }

                        }
                    }
                } finally {
                    TX.TXend();
                }
            } catch (TXLibExceptions.AbortException exp) {
                System.out.println("abort");
                continue;
            }
            break;
        }
    }
}