package edu.ucsb.eucalyptus.cloud.ws;

import com.eucalyptus.context.Contexts;
import com.eucalyptus.ws.handlers.SbxRate;
import edu.ucsb.eucalyptus.util.SbxRequestPool;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mac
 * Date: 2013/6/18
 * Time:  2:10
 * To change this template use File | Settings | File Templates.
 */
public class SbxControlThread extends Thread {
    private static Logger LOG = Logger.getLogger( SbxControlThread.class );
    public static int MAXJOBTHREAD = System.getProperty("euca.sbxrequest.jobthread")==null ? 100 : Integer.valueOf(System.getProperty("euca.sbxrequest.jobthread"));
    public static int MAXPUTTHREAD = System.getProperty("euca.sbxrequest.putthread")==null ? 100 : Integer.valueOf(System.getProperty("euca.sbxrequest.putthread"));
    public static int MAXGETTHREAD = System.getProperty("euca.sbxrequest.getthread")==null ? 100 : Integer.valueOf(System.getProperty("euca.sbxrequest.getthread"));
    private static SbxRequestPool reqpool = new SbxRequestPool();
    private static JobSelectionThread bossThread = null;
    private static JobThread[] workerThreads = null;
    private static PutJobThread[] putThreads = null;
    private static GetJobThread[] getThreads = null;
//    private static CleanThread cleanThread = null;
    private static TimeoutThread timeoutThread = null;
    private static SbxJobLogThread sbxjobThread = null;
    private static boolean bRun = true;

    public static void init() {
        bossThread = new JobSelectionThread(reqpool);
        bossThread.setName("SbxBossThread");
        workerThreads = new JobThread[MAXJOBTHREAD];
        for (int i=0; i<MAXJOBTHREAD; i++) {
            workerThreads[i] = new JobThread();
            workerThreads[i].setName("SbxWorkerThread" + i);
        }
        putThreads = new PutJobThread[MAXPUTTHREAD];
        for (int i=0; i<MAXPUTTHREAD; i++) {
            putThreads[i] = new PutJobThread();
            putThreads[i].setName("SbxPutThread"+i);
        }
        getThreads = new GetJobThread[MAXGETTHREAD];
        for (int i=0; i<MAXGETTHREAD; i++) {
            getThreads[i] = new GetJobThread();
            getThreads[i].setName("SbxGetThread"+i);
        }
//        cleanThread = new CleanThread();
//        cleanThread.setName("SbxCleanThread");
        timeoutThread = new TimeoutThread();
        timeoutThread.setName("SbxTimeoutThread");
        sbxjobThread = new SbxJobLogThread();
        sbxjobThread.setName("SbxJobLogThread");
    }

    public void run() {
        if (bossThread == null) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread error : JobSelectionThread is null : call init() before run()");
            return;
        }
        if (workerThreads == null) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread error : JobThreads is null : call init() before run()");
            return;
        }
        if (putThreads == null) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread error : PutJobThreads is null : call init() before run()");
            return;
        }
        if (getThreads == null) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread error : GetJobThreads is null : call init() before run()");
            return;
        }
//        if (cleanThread == null) {
//            LOG.debug("THROTTLINGandRETRY: SbxControlThread error : CleanThread is null : call init() before run()");
//            return;
//        }
        if (timeoutThread == null) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread error : TimeoutThread is null : call init() before run()");
            return;
        }
        if (sbxjobThread == null) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread error : SbxJobLogThread is null : call init() before run()");
            return;
        }
        try {
            bossThread.start();
            for (int i=0; i<MAXJOBTHREAD; i++) {
                workerThreads[i].start();
            }
            for (int i=0; i<MAXPUTTHREAD; i++) {
                putThreads[i].start();
            }
            for (int i=0; i<MAXGETTHREAD; i++) {
                getThreads[i].start();
            }
//            cleanThread.start();
            timeoutThread.start();
            sbxjobThread.start();
            int duration = 10; //sec
            while (bRun) {
                TimeUnit.SECONDS.sleep(duration);
                if (Contexts.SBXSVR) {
                    //calculate empty rate
                    SbxRate.calculateRate(duration);
                }
            }
        } catch (InterruptedException e) {
            bRun = false;
        } catch (Throwable t) {
            LOG.debug("THROTTLINGandRETRY: SbxControlThread.run error : " + t);
        }
    }//run
}
