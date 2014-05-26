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

package pt.uminho.di.raft.service.operations;

import org.ws4d.java.service.Operation;
import org.ws4d.java.types.QName;
import pt.uminho.di.raft.entities.Server;

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
