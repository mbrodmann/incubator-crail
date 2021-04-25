/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.storage.tcp;

import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.crail.CrailBuffer;
import org.apache.crail.CrailStore;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.core.CoreDataStore;
import org.apache.crail.memory.OffHeapBuffer;
import org.apache.crail.metadata.BlockInfo;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.RelocationBlockInfo;
import org.apache.crail.rpc.RpcClient;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcDispatcher;
import org.apache.crail.storage.*;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCServerChannel;
import com.ibm.narpc.NaRPCServerEndpoint;
import com.ibm.narpc.NaRPCServerGroup;
import com.ibm.narpc.NaRPCService;

public class TcpStorageServer implements Runnable, StorageServer, NaRPCService<TcpStorageRequest, TcpStorageResponse> {
	private static final Logger LOG = CrailUtils.getLogger();

	private NaRPCServerGroup<TcpStorageRequest, TcpStorageResponse> serverGroup;
	private NaRPCServerEndpoint<TcpStorageRequest, TcpStorageResponse> serverEndpoint;
	private InetSocketAddress address;
	private boolean alive;
	private long regions;
	private long keys;
	private ConcurrentHashMap<Integer, ByteBuffer> dataBuffers;
	private String dataDirPath;
	private boolean relocationOngoing;

	// WIP: direct data transfer from DN to DN
	private CrailStore store;
	private CrailConfiguration conf;
	private StorageClient storageClient;
	private RpcConnection rpcConnection;


	@Override
	public void init(CrailConfiguration conf, String[] args) throws Exception {
		TcpStorageConstants.init(conf, args);

		this.serverGroup = new NaRPCServerGroup<TcpStorageRequest, TcpStorageResponse>(this, TcpStorageConstants.STORAGE_TCP_QUEUE_DEPTH, (int) CrailConstants.BLOCK_SIZE*2, false, TcpStorageConstants.STORAGE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		this.address = StorageUtils.getDataNodeAddress(TcpStorageConstants.STORAGE_TCP_INTERFACE, TcpStorageConstants.STORAGE_TCP_PORT);
		serverEndpoint.bind(address);
		this.alive = false;
		this.regions = TcpStorageConstants.STORAGE_TCP_STORAGE_LIMIT/TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE;
		this.keys = 0;
		this.dataBuffers = new ConcurrentHashMap<Integer, ByteBuffer>();
		this.dataDirPath = StorageUtils.getDatanodeDirectory(TcpStorageConstants.STORAGE_TCP_DATA_PATH, address);
		this.relocationOngoing = false;
		StorageUtils.clean(TcpStorageConstants.STORAGE_TCP_DATA_PATH, dataDirPath);

		// WIP: DN to DN transer
		this.conf = CrailConfiguration.createConfigurationFromFile();
		this.store = store = CrailStore.newInstance(conf);
		storageClient = StorageClient.createInstance(conf.get("crail.storage.types"));
		storageClient.init(store.getStatistics(), ((CoreDataStore)store).getBufferCache(), conf, null);

		RpcClient rpcClient = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);

		rpcClient.init(conf, null);

		ConcurrentLinkedQueue<InetSocketAddress> namenodeList = CrailUtils.getNameNodeList();
		ConcurrentLinkedQueue<RpcConnection> connectionList = new ConcurrentLinkedQueue<RpcConnection>();
		while(!namenodeList.isEmpty()){
			InetSocketAddress address = namenodeList.poll();
			RpcConnection connection = rpcClient.connect(address);
			connectionList.add(connection);
		}
		rpcConnection = connectionList.peek();
		if (connectionList.size() > 1){
			rpcConnection = new RpcDispatcher(connectionList);
		}
	}

	@Override
	public void printConf(Logger logger) {
		TcpStorageConstants.printConf(logger);
	}

	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource resource = null;
		if (keys < regions){
			int fileId = (int) keys++;
			String dataFilePath = Paths.get(dataDirPath, Integer.toString(fileId)).toString();
			RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
			FileChannel dataChannel = dataFile.getChannel();
			ByteBuffer buffer = dataChannel.map(MapMode.READ_WRITE, 0, TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE);
			dataBuffers.put(fileId, buffer);
			dataFile.close();
			dataChannel.close();
			long address = CrailUtils.getAddress(buffer);
			resource = StorageResource.createResource(address, buffer.capacity(), fileId);
		}
		return resource;
	}

	@Override
	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public boolean isAlive() {
		return alive;
	}

	@Override
	public void prepareToShutDown(){

		LOG.info("Preparing TCP-Storage server for shutdown");

		this.alive = false;

		try {
			serverEndpoint.close();
			serverGroup.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {
			LOG.info("running TCP storage server, address " + address);
			this.alive = true;
			while(true){
				NaRPCServerChannel endpoint = serverEndpoint.accept();
				LOG.info("new connection " + endpoint.address());
			}
		} catch(Exception e){
			// if StorageServer is still marked as running output stacktrace
			// otherwise this is expected behaviour
			if(this.alive) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public TcpStorageRequest createRequest() {
		return new TcpStorageRequest();
	}

	@Override
	public TcpStorageResponse processRequest(TcpStorageRequest request) {
		if (request.type() == TcpStorageProtocol.REQ_WRITE){
			if(this.relocationOngoing) {
				TcpStorageResponse.WriteResponse writeResponse = new TcpStorageResponse.WriteResponse(-1);
				return new TcpStorageResponse(writeResponse);
			}
			TcpStorageRequest.WriteRequest writeRequest = request.getWriteRequest();
			ByteBuffer buffer = dataBuffers.get(writeRequest.getKey()).duplicate();
			long offset = writeRequest.getAddress() - CrailUtils.getAddress(buffer);
//			LOG.info("processing write request, key " + writeRequest.getKey() + ", address " + writeRequest.getAddress() + ", length " + writeRequest.length() + ", remaining " + writeRequest.getBuffer().remaining() + ", offset " + offset);
			buffer.clear().position((int) offset);
			buffer.put(writeRequest.getBuffer());
			TcpStorageResponse.WriteResponse writeResponse = new TcpStorageResponse.WriteResponse(writeRequest.length());
			return new TcpStorageResponse(writeResponse);
		} else if (request.type() == TcpStorageProtocol.REQ_READ){
			TcpStorageRequest.ReadRequest readRequest = request.getReadRequest();
			ByteBuffer buffer = dataBuffers.get(readRequest.getKey()).duplicate();
			long offset = readRequest.getAddress() - CrailUtils.getAddress(buffer);
//			LOG.info("processing read request, address " + readRequest.getAddress() + ", length " + readRequest.length() + ", offset " + offset);
			long limit = offset + readRequest.length();
			buffer.clear().position((int) offset).limit((int) limit);
			TcpStorageResponse.ReadResponse readResponse = new TcpStorageResponse.ReadResponse(buffer);
			return new TcpStorageResponse(readResponse);
		} else {
			LOG.info("processing unknown request");
			return new TcpStorageResponse(TcpStorageProtocol.RET_RPC_UNKNOWN);
		}
	}

	@Override
	public void addEndpoint(NaRPCServerChannel channel){
	}

	@Override
	public void removeEndpoint(NaRPCServerChannel channel){
	}

	@Override
	public void setRelocationOngoing() {

		try {
			this.relocationOngoing = true;

			long start = System.currentTimeMillis();

			// datanode information indicating that the list of occupied blocks should be transferred
			DataNodeInfo dnInfo = new DataNodeInfo(0,0,-1, address.getAddress().getAddress(), address.getPort());

			// list of all blocks still stored on datanode
			LinkedList<RelocationBlockInfo> blocks = rpcConnection.getDataNode(dnInfo).get().getBlocks();

			int blocksize = Integer.parseInt(conf.get("crail.blocksize"));

			// transfer all blocks to new datanodes
			// !!! for now we assume now concurrent data operations !!!

			for(RelocationBlockInfo blockInfo: blocks) {

				boolean isLast = blockInfo.getIsLast() == 1;
				short index = blockInfo.getIndex();
				long capacity = blockInfo.getCapacity();
				long fd = blockInfo.getFd();

				// load block data into local buffer
				int limit;
				if(isLast) {
					limit = (int) (capacity % blocksize);
					if(limit == 0) {
						limit = Math.min(blocksize, (int) capacity);
					}
				} else {
					limit = Math.min(blocksize, (int) capacity);
				}

				// request new block that replaces former block
				// the index of the block was already communicated by the namenode
				// this special situation is treadted accordingly in the namenode
				BlockInfo targetBlock = rpcConnection.getBlock(fd, -1, index, capacity).get().getBlockInfo();
				DataNodeInfo target = targetBlock.getDnInfo();

				// only perform read and write when capacity is positive
				if(limit > 0) {
					
					ByteBuffer buffer = dataBuffers.get(blockInfo.getLkey()).duplicate();
					long offset = blockInfo.getAddr() - CrailUtils.getAddress(buffer);
					buffer.clear().position((int) offset);
					
					CrailBuffer crailBuffer = new OffHeapBuffer(buffer);

					StorageEndpoint targetEndpoint = ((CoreDataStore)store).getDatanodeEndpointCache().getDataEndpoint(target);

					buffer.limit(buffer.position() + limit);

					StorageFuture writeFuture = targetEndpoint.write(crailBuffer, targetBlock, 0);
					writeFuture.get();
				}

				// finally replace old block with new block in namenode
				rpcConnection.getBlock(fd, -2, index, capacity).get().getBlockInfo();

			}

			long end = System.currentTimeMillis();
			long time = end - start;
			System.out.println("Finished block relocation process after " + time + " ms");



		} catch(Exception e) {
			e.printStackTrace();
		}


	}

	@Override
	public void ackReadyForRelocation(StorageRpcClient storageRpc) throws Exception {
		storageRpc.ackRejectWrites();
	}
}
