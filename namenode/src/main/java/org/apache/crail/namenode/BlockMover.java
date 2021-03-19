package org.apache.crail.namenode;

import org.apache.crail.metadata.DataNodeInfo;

public class BlockMover implements Runnable {

NameNodeService service;
DataNodeInfo info;

BlockMover(NameNodeService service, DataNodeInfo info) {
    this.service = service;
    this.info = info;
}

@Override
public void run() {
    
    try {
        service.removeDataNodeCompletely(this.info);
    } catch(Exception e) {
        e.printStackTrace();
    }
}

}
