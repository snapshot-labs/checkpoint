import { Log } from 'viem';
import { CheckpointRecord } from '../../../stores/checkpoints';
import { ContractSourceConfig } from '../../../types';
import { BlockFetcher, FetchedBlock } from './types';
import { RpcBlockFetcher } from './rpc';

type HypersyncLog = {
  block_number?: number;
  log_index?: number;
  transaction_index?: number;
  transaction_hash?: string;
  block_hash?: string;
  address?: string;
  data?: string;
  topic0?: string | null;
  topic1?: string | null;
  topic2?: string | null;
  topic3?: string | null;
  removed?: boolean;
};

type HypersyncBlock = {
  number?: number;
  timestamp?: number;
  hash?: string;
  parent_hash?: string;
};

type HypersyncResponse = {
  next_block: number;
  archive_height?: number;
  data: {
    blocks: HypersyncBlock[];
    logs: HypersyncLog[];
  };
};

const FIELD_SELECTION = {
  block: ['number', 'timestamp', 'hash', 'parent_hash'],
  log: [
    'block_number',
    'log_index',
    'transaction_index',
    'transaction_hash',
    'block_hash',
    'address',
    'data',
    'topic0',
    'topic1',
    'topic2',
    'topic3',
    'removed'
  ]
};

export class HypersyncBlockFetcher implements BlockFetcher {
  private url: string | null = null;
  private readonly apiToken: string;
  private readonly rpcFetcher: RpcBlockFetcher;
  private readonly blockCache = new Map<number, FetchedBlock>();

  constructor({
    apiToken,
    rpcUrl
  }: {
    apiToken: string;
    rpcUrl: string;
  }) {
    this.apiToken = apiToken;
    this.rpcFetcher = new RpcBlockFetcher(rpcUrl);
  }

  private async getUrl(): Promise<string> {
    if (!this.url) {
      const chainId = await this.rpcFetcher.getChainId();
      this.url = `https://${chainId}.hypersync.xyz`;
    }

    return this.url;
  }

  getCachedBlocks(): Map<number, FetchedBlock> {
    return this.blockCache;
  }

  async getChainId(): Promise<number> {
    return this.rpcFetcher.getChainId();
  }

  async getLatestBlockNumber(): Promise<number> {
    const url = await this.getUrl();
    const res = await fetch(`${url}/height`);
    if (!res.ok) {
      throw new Error(`HyperSync height request failed: ${res.statusText}`);
    }

    const data = await res.json();

    return data.height;
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

    let allLogs: HypersyncLog[] = [];
    let currentFrom = fromBlock;
    const exclusiveToBlock = toBlock + 1;

    while (currentFrom < exclusiveToBlock) {
      const response = await this.query({
        from_block: currentFrom,
        to_block: exclusiveToBlock,
        logs: [
          {
            address: addresses,
            topics: topic0 ? [topic0] : undefined
          }
        ],
        field_selection: FIELD_SELECTION
      });

      this.cacheBlocks(response.data.blocks);
      allLogs = allLogs.concat(response.data.logs);

      if (response.next_block >= exclusiveToBlock) break;
      currentFrom = response.next_block;
    }

    return this.convertLogs(allLogs);
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

  private async query(body: Record<string, unknown>): Promise<HypersyncResponse> {
    const url = await this.getUrl();
    const res = await fetch(`${url}/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.apiToken}`
      },
      body: JSON.stringify(body)
    });

    if (!res.ok) {
      throw new Error(`HyperSync query failed: ${res.statusText}`);
    }

    return res.json();
  }

  private cacheBlocks(blocks: HypersyncBlock[]): void {
    for (const block of blocks) {
      if (
        block.number != null &&
        block.timestamp != null &&
        block.hash != null &&
        block.parent_hash != null
      ) {
        this.blockCache.set(block.number, {
          number: block.number,
          hash: block.hash,
          parentHash: block.parent_hash,
          timestamp: block.timestamp
        });
      }
    }
  }

  private convertLogs(hypersyncLogs: HypersyncLog[]): Log[] {
    return hypersyncLogs.map(log => {
      const topics: `0x${string}`[] = [];
      if (log.topic0) topics.push(log.topic0 as `0x${string}`);
      if (log.topic1) topics.push(log.topic1 as `0x${string}`);
      if (log.topic2) topics.push(log.topic2 as `0x${string}`);
      if (log.topic3) topics.push(log.topic3 as `0x${string}`);

      return {
        address: (log.address ?? '0x') as `0x${string}`,
        blockHash: (log.block_hash ?? null) as `0x${string}` | null,
        blockNumber: log.block_number != null ? BigInt(log.block_number) : null,
        data: (log.data ?? '0x') as `0x${string}`,
        logIndex: log.log_index ?? 0,
        transactionHash: (log.transaction_hash ?? null) as `0x${string}` | null,
        transactionIndex: log.transaction_index ?? 0,
        removed: log.removed ?? false,
        topics
      } as Log;
    });
  }
}
