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
                    //System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output_" + name + ".txt"))));
                    TX.TXbegin();
                    for (int i = 0; i < keyList.size(); i++) {
                        String op = operation;
                        if(operation.equals("mixed")){
                            if(i % 10 < 10*INSERT_PERCENT) op = "insert";
                            else op = "remove";
                        }
                        if (op.equals("insert")) {
                            TX.print("insert: " + name + ", TXnum: " + TX.lStorage.get().TxNum + ", key: " + keyList.get(i));
                            tree.insert(keyList.get(i), valueList.get(i));
                        } else if (op.equals("remove")) {
                            TX.print("remove: " + name);
                            if (validOperation) {
                                tree.remove(keyList.get(i));
                            } else {
                                int finalI = i;
                                Assert.assertThrows(AssertionError.class, () -> tree.remove(keyList.get(finalI)));
                            }
                        } else { // lookup
                            TX.print("lookup: " + name);
                            if (validOperation) {
                                Assert.assertEquals(valueList.get(i), tree.lookup(keyList.get(i)));
                            } else {
                                Assert.assertNull(tree.lookup(keyList.get(i)));
                            }

                        }
                    }
                } catch (TXLibExceptions.AbortException exp){
                    TX.print("********__FAILED__********");
                    //System.exit(1);
                } finally {
                    TX.TXend();
                    tree.debugCheckRebuild();
                    tree.checkLevels();
                }
            } catch (TXLibExceptions.AbortException exp) {
                TX.print("abort");
                //continue;
            }
            break;
        }
    }
}