import java.util.List;

public class ISTTask implements  Runnable{
    List <ISTOperation> operations;
    IST tree;
    ISTTask (List<ISTOperation> operations,IST tree){
        this.tree = tree;
        this.operations = operations;

    }
    @Override
    public void run() {
        while (true){
            try {
                try {
                    TX.TXbegin();
                    if(TX.DEBUG_MODE_IST) {
                        System.out.println("Starting task #" + TX.lStorage.get().TxNum + ", TID = " + Thread.currentThread().getId());
                    }
                    for (ISTOperation operation: operations){
                        switch (operation.type){
                            case "insert":
                                if(TX.DEBUG_MODE_IST) {
                                    System.out.println("insert  , TXnum: " + TX.lStorage.get().TxNum + ", key: " + operation.key);
                                }
                                tree.insert(operation.key, operation.value);
                                break;
                            case "remove":
                                if(TX.DEBUG_MODE_IST) {
                                    System.out.println("remove  , TXnum: " + TX.lStorage.get().TxNum + ", key: " + operation.key);
                                }
                                tree.remove(operation.key);
                                break;
                            case "lookup":
                                if(TX.DEBUG_MODE_IST) {
                                    System.out.println("lookup  , TXnum: " + TX.lStorage.get().TxNum + ", key: " + operation.key);
                                }
                                tree.lookup(operation.key);
                                break;
                        }
                    }

                } finally {
                    TX.TXend();
                    if(TX.DEBUG_MODE_IST) {
                        tree.checkLevels();
                    }
                }
            } catch (TXLibExceptions.AbortException exp) {
                TX.print("abort");
                continue;
            }
            break;
        }
    }
}
