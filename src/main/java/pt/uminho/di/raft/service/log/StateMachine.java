/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.service.log;

/**
 *
 * @author fcampos
 */
public interface StateMachine {

    void initialize(String id);

    void initializeDB();

    boolean insertIntoDB(LogEntry entry);

    void shutdown();

}
