/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.entities.workers;

import org.apache.log4j.Logger;
import pt.uminho.di.raft.service.log.Log;

/**
 *
 * @author fcampos
 */
public class CommittingTask /* extends ServerWorkingTask */ extends Thread {

    static Logger logger = Logger.getLogger(CommittingTask.class);

    private Log log;
    private String idString;

    public CommittingTask(Log log, String str)
    {
        this.log = log;
        idString = str;
    }

    @Override
    public void run()
    {
        logger.debug(idString + "Committing Log Entries...");
        log.checkCommittedEntries();
        logger.debug(idString + "Committing Log Entries ended.");
    }
}
