/*******************************************************************************
 * Copyright (c) 2014 Filipe Campos.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package pt.uminho.di.raft.entities.states;

import pt.uminho.di.raft.entities.*;
import org.apache.log4j.Logger;

public abstract class ServerStateTask {

    static Logger logger = Logger.getLogger(ServerStateTask.class);

    boolean running = true;
    Server server;

    public ServerStateTask(Server s)
    {
        server = s;
    }

    public boolean isRunning()
    {
        return running;
    }

    public void setRunning(boolean running)
    {
//        logger.debug(server.getIdString() + this.toString() + " Set running to " + running);
        this.running = running;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }
    
    public abstract void timeout();

    public void terminate()
    {
        setRunning(false);
    }

    public abstract void heartbeat();

    public abstract void execute();
}
