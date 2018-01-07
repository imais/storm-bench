import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Map;

import javax.management.JMException;
import sun.tools.jconsole.LocalVirtualMachine;
import org.apache.log4j.Logger;


public class JmxMonitor {
    static private String DEFAULT_BEANS_FILE = "./beans";
    static private int DEFAULT_SAMPLE_INTERVAL_SEC = 3;
    
    private int pid_;
    private JmxClient client_;
    private ArrayList<String[]> beanAttrList_;
    private long startTime_;
    private int intervalSec_;

    public JmxMonitor(String className, String beansFile, int intervalSec) {
        pid_ = getPid(className);
        if (pid_ < 0) {
            System.err.println("pid not found for " + className);
            System.exit(1);
        }

        beanAttrList_ = new ArrayList<String[]>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(beansFile));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] beanAttr = line.split("#");
                beanAttrList_.add(beanAttr);
            }
            reader.close();
        } catch  (Exception ex) {
            ex.printStackTrace();
        }
        // for (String[] beanAttr : beanAttrList_)
        //     System.out.println(beanAttr[0] + ", " + beanAttr[1]);
        intervalSec_ = intervalSec;

        client_ = new JmxClient(pid_);
        startTime_ = System.currentTimeMillis();
    }

    private int getPid(String className) {
        int pid = -1;

        Map<Integer, LocalVirtualMachine> vms = LocalVirtualMachine.getAllVirtualMachines();
        for (Map.Entry<Integer, LocalVirtualMachine> entry : vms.entrySet()) {
            LocalVirtualMachine vm = entry.getValue();
            // if (vm.displayName().startsWith(className)) {
            if (vm.displayName().contains(className) && 
                !vm.displayName().contains(JmxMonitor.class.getName())) {
                pid = vm.vmid();
                System.out.println("Found vm \"" + vm.displayName() + "\" with pid " + pid);
                break;
            }
        }

        return pid;
    }

    public void doMonitor() {
        try {
            client_.open();

            while (true) {
                String str = System.currentTimeMillis() + ", ";

                for (String[] beanAttr : beanAttrList_) {
                    Map<String, Object> vals = client_
                        .getAttributeValues(beanAttr[0], beanAttr[1]);
                    for (Map.Entry<String, Object> val : vals.entrySet()) {
                        if (val.getValue() instanceof Double)
                            str += String.format("%.3f", val.getValue()) + ", ";
                        else
                            str += val.getValue() + ", ";
                    }
                }
                str = str.substring(0, str.lastIndexOf(','));
                System.out.println(str);
                Thread.sleep(intervalSec_ * 1000);
            }

            // client_.close();
        } catch (InterruptedException ex) {
            System.err.println(ex);
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (JMException ex) {
            System.err.println(ex);
        }
    }

    public static void main(String[] args) {
        String className = null;
        String beansFile = DEFAULT_BEANS_FILE;
        int intervalSec = DEFAULT_SAMPLE_INTERVAL_SEC;

        if (args.length < 1 || 3 < args.length) {
            System.err.println("Usage: java JmxMonitor [monitoring class name] [beans file] [interval (sec)]");
            System.exit(1);
        }
        if (1 <= args.length)
            className = args[0];
        if (2 <= args.length)
            beansFile = args[1];
        if (3 <= args.length)
            intervalSec = Integer.parseInt(args[2]);

        JmxMonitor monitor  = new JmxMonitor(className, beansFile, intervalSec);
        monitor.doMonitor();
    }
}
