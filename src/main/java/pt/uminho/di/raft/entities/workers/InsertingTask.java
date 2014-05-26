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

package pt.uminho.di.raft.entities.workers;

import org.apache.log4j.Logger;
import org.ws4d.java.types.EndpointReference;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.entities.ServerClient;

public class InsertingTask extends ServerWorkingTask {

    static Logger logger = Logger.getLogger(InsertingTask.class);

    private ServerClient serverClient;
    private EndpointReference epr;

    public InsertingTask(ServerClient cli, Server srv, EndpointReference e)
    {
        super(srv);
        serverClient = cli;
        epr = e;
    }

    @Override
    public void run()
    {
        logger.debug(getServer().getIdString() + "Inserting Task started for EPR:" + epr);
        serverClient.insertServer(epr);
        logger.debug(getServer().getIdString() + "Inserting Task ended.");
    }
}
