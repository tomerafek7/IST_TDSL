import org.junit.Assert;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

public class ISTComplexRun implements Runnable {

    String name;
    IST tree;
    List<Integer> keyList;
    List<Integer> valueList;
    List<Integer> keyList2;
    List<Integer> valueList2;
    String operation;
    boolean validOperation;
    static double INSERT_PERCENT = 0.5;
    static int NUM_OPS_PER_TX = 20;

    public ISTComplexRun(String name, IST tree, List<Integer> keyList, List<Integer> valueList,
                         List<Integer> keyList2, List<Integer> valueList2, String operation, boolean validOperation){
        this.name = name;
        this.tree = tree;
        this.keyList = keyList;
        this.valueList = valueList;
        this.keyList2 = keyList2;
        this.valueList2 = valueList2;
        this.operation = operation;
        this.validOperation = validOperation;
    }

    @Override
    public void run() {
        for (int i = 0; i < keyList.size() / NUM_OPS_PER_TX; i++) {
            while (true) {
                try {
                    try {
                        //System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output_" + name + ".txt"))));
                        TX.TXbegin();
                        for (int j = 0; j < NUM_OPS_PER_TX; j++) {
                            if (operation.equals("insert")) {
                                TX.print("insert: " + name + ", TXnum: " + TX.lStorage.get().TxNum + ", key: " + keyList.get(i * NUM_OPS_PER_TX + j));
                                tree.insert(keyList.get(i * NUM_OPS_PER_TX + j), valueList.get(i * NUM_OPS_PER_TX + j));
                                Assert.assertEquals(valueList.get(i * NUM_OPS_PER_TX + j), tree.lookup(keyList.get(i * NUM_OPS_PER_TX + j)));
                            } else if (operation.equals("remove")) {
                                TX.print("remove: " + name);
                                if (validOperation) {
                                    tree.remove(keyList.get(i * NUM_OPS_PER_TX + j));
                                } else {
                                    int finalI = i;
                                    Assert.assertThrows(AssertionError.class, () -> tree.remove(keyList.get(finalI)));
                                }
                            } else if (operation.equals("lookup")){
                                TX.print("lookup: " + name);
                                if (validOperation) {
                                    Assert.assertEquals(valueList.get(i * NUM_OPS_PER_TX + j), tree.lookup(keyList.get(i * NUM_OPS_PER_TX + j)));
                                } else {
                                    Assert.assertNull(tree.lookup(keyList.get(i * NUM_OPS_PER_TX + j)));
                                }

                            } else { // mixed (insert+remove)
                                TX.print("insert: " + name + ", TXnum: " + TX.lStorage.get().TxNum + ", key: " + keyList.get(i * NUM_OPS_PER_TX + j));
                                tree.insert(keyList.get(i * NUM_OPS_PER_TX + j), valueList.get(i * NUM_OPS_PER_TX + j));
                                TX.print("remove: " + name);
                                tree.remove(keyList2.get(i * NUM_OPS_PER_TX + j));
                            }
                        }
                    } catch (TXLibExceptions.AbortException exp) {
                        TX.print("********__FAILED__********");
                        //System.exit(1);
                    } finally {
                        TX.TXend();
                        tree.debugCheckRebuild();
                        tree.checkLevels();
                    }
                } catch (TXLibExceptions.AbortException exp) {
                    TX.print("abort");
                    continue;
                }
                break;
            }
            //break;
        }
    }
}