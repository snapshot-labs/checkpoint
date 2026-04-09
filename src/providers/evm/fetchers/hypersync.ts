import {
  HypersyncClient,
  type Query,
  type Log as HypersyncLog
} from '@envio-dev/hypersync-client';
import { type Log } from 'viem';
import { CheckpointRecord } from '../../../stores/checkpoints';
import { ContractSourceConfig } from '../../../types';
import { BlockFetcher, FetchedBlock } from './types';
import { RpcBlockFetcher } from './rpc';

export class HypersyncBlockFetcher implements BlockFetcher {
  private readonly hypersync: HypersyncClient;
  private readonly rpcFetcher: RpcBlockFetcher;
  private readonly blockCache = new Map<number, FetchedBlock>();

  constructor({
    chainId,
    apiToken,
    rpcUrl
  }: {
    chainId: number;
    apiToken: string;
    rpcUrl: string;
  }) {
    this.hypersync = new HypersyncClient({
      url: `https://${chainId}.hypersync.xyz`,
      apiToken
    });
    this.rpcFetcher = new RpcBlockFetcher(rpcUrl);
  }

  getCachedBlocks(): Map<number, FetchedBlock> {
    return this.blockCache;
  }

  async getChainId(): Promise<number> {
    return this.rpcFetcher.getChainId();
  }

  async getLatestBlockNumber(): Promise<number> {
    return this.hypersync.getHeight();
  }

  async getBlock(blockNumber: number): Promise<FetchedBlock> {
    return this.rpcFetcher.getBlock(blockNumber);
  }

  async getBlockHash(blockNumber: number): Promise<string> {
    return this.rpcFetcher.getBlockHash(blockNumber);
  }

  async getLogsByBlockHash(blockHash: string): Promise<Log[]> {
    return this.rpcFetcher.getLogsByBlockHash(blockHash);
  }

  async getLogs(
    fromBlock: number,
    toBlock: number,
    address: string | string[],
    topics: (string | string[])[] = []
  ): Promise<Log[]> {
    const addresses = Array.isArray(address) ? address : [address];
    const topic0 =
      topics.length > 0
        ? Array.isArray(topics[0])
          ? topics[0]
          : [topics[0]]
        : undefined;

    const query: Query = {
      fromBlock,
      toBlock: toBlock + 1, // HyperSync toBlock is exclusive
      logs: [
        {
          address: addresses,
          topics: topic0 ? [topic0] : undefined
        }
      ],
      fieldSelection: {
        log: [
          'BlockNumber',
          'TransactionHash',
          'TransactionIndex',
          'BlockHash',
          'Address',
          'Data',
          'LogIndex',
          'Topic0',
          'Topic1',
          'Topic2',
          'Topic3',
          'Removed'
        ],
        block: ['Number', 'Timestamp', 'Hash', 'ParentHash']
      }
    };

    const response = await this.hypersync.collect(query, {});

    for (const block of response.data.blocks) {
      if (
        block.number != null &&
        block.timestamp != null &&
        block.hash != null &&
        block.parentHash != null
      ) {
        this.blockCache.set(block.number, {
          number: block.number,
          hash: block.hash,
          parentHash: block.parentHash,
          timestamp: block.timestamp
        });
      }
    }

    return this.convertHypersyncLogs(response.data.logs);
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number,
    sources: ContractSourceConfig[],
    getEventHash: (name: string) => string
  ): Promise<{ checkpoints: CheckpointRecord[]; logs: Log[] }> {
    const chunks: ContractSourceConfig[][] = [];
    for (let i = 0; i < sources.length; i += 20) {
      chunks.push(sources.slice(i, i + 20));
    }

    let allLogs: Log[] = [];
    for (const chunk of chunks) {
      const addresses = chunk.map(source => source.contract);
      const topics = chunk.flatMap(source =>
        source.events.map(event => getEventHash(event.name))
      );
      const logs = await this.getLogs(fromBlock, toBlock, addresses, [topics]);
      allLogs = allLogs.concat(logs);
    }

    const checkpoints = allLogs.map(log => ({
      blockNumber: Number(log.blockNumber),
      contractAddress: log.address
    }));

    return { checkpoints, logs: allLogs };
  }

  private convertHypersyncLogs(hypersyncLogs: HypersyncLog[]): Log[] {
    return hypersyncLogs.map(log => {
      const topics: `0x${string}`[] = [];
      for (const topic of log.topics) {
        if (topic) topics.push(topic as `0x${string}`);
      }

      return {
        address: log.address as `0x${string}`,
        blockHash: log.blockHash as `0x${string}`,
        blockNumber: log.blockNumber != null ? BigInt(log.blockNumber) : null,
        data: (log.data ?? '0x') as `0x${string}`,
        logIndex: log.logIndex ?? 0,
        transactionHash: log.transactionHash as `0x${string}`,
        transactionIndex: log.transactionIndex ?? 0,
        removed: log.removed ?? false,
        topics
      } as Log;
    });
  }
}
