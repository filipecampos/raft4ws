/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.service.operations;

import org.ws4d.java.service.Operation;
import org.ws4d.java.types.QName;
import pt.uminho.di.raft.entities.Server;

/**
 *
 * @author fcampos
 */
public abstract class RaftOperation extends Operation {

    protected Server server;

    public RaftOperation(Server s, String opName, QName portType)
    {
        super(opName, portType);
        server = s;

        initInput();
        initOutput();
    }

    public abstract void initInput();

    public abstract void initOutput();
}
