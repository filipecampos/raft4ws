Raft4WS - Raft for Web Services
=======

Raft4WS is an implementation of the [Raft distributed consensus protocol]
(https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf "In Search of an Understandable Consensus Algorithm")
on the 
[WS4D](http://ws4d.org/ "Web Services for Devices") 
[JMEDS Framework v2.0 beta10](http://ws4d.e-technik.uni-rostock.de/jmeds/ "WS4D.org Java Multi Edition DPWS Stack")

## Release History

* **0.1**: Initial release. (26 May, 2014)


## Features

Raft4WS implements the following features of Raft:
- Leader election
- Log replication

Advanced features, such as log compaction or online cluster reconfiguration, still don't have an implementation timeline.

## Installing and using Raft4WS 

Follow the instructions on the INSTALL file to build the binary distribution package.
It will be located at ./target/raft4ws-0.1-bin.zip.
Extract the contents of the zip file and follow the following instructions to execute a Raft server or client.

- To run a Raft server:

 $ ./bin/server <id> <timeout(ms)>

- To run a Raft client:

 $ ./bin/client
