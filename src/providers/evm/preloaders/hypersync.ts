import { Log } from 'viem';
import { FetchedBlock, Preloader } from './types';
import { ContractSourceConfig } from '../../../types';
import { chunk } from '../../../utils/helpers';

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

export class HypersyncPreloader implements Preloader {
  private readonly url: string;
  private readonly apiToken: string;

  constructor({ apiToken, chainId }: { apiToken: string; chainId: number }) {
    this.apiToken = apiToken;
    this.url = `https://${chainId}.hypersync.xyz`;
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number,
    sources: ContractSourceConfig[],
    getEventHash: (name: string) => string
  ): Promise<{
    logs: Log[];
    blocks: FetchedBlock[];
  }> {
    const allLogs: Log[] = [];
    const allBlocks: FetchedBlock[] = [];

    for (const sourceChunk of chunk(sources, 20)) {
      const addresses = sourceChunk.map(source => source.contract);
      const topics = sourceChunk.flatMap(source =>
        source.events.map(event => getEventHash(event.name))
      );

      const { logs, blocks } = await this.fetchLogs(
        fromBlock,
        toBlock,
        addresses,
        [topics]
      );
      for (const log of logs) allLogs.push(log);
      for (const block of blocks) allBlocks.push(block);
    }

    return { logs: allLogs, blocks: allBlocks };
  }

  private async fetchLogs(
    fromBlock: number,
    toBlock: number,
    addresses: string[],
    topics: (string | string[])[]
  ): Promise<{ logs: Log[]; blocks: FetchedBlock[] }> {
    const topic0 =
      topics.length > 0
        ? Array.isArray(topics[0])
          ? topics[0]
          : [topics[0]]
        : undefined;

    const allLogs: HypersyncLog[] = [];
    const allBlocks: FetchedBlock[] = [];
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

      for (const block of this.convertBlocks(response.data.blocks))
        allBlocks.push(block);
      for (const log of response.data.logs) allLogs.push(log);

      if (response.next_block >= exclusiveToBlock) break;
      currentFrom = response.next_block;
    }

    return { logs: this.convertLogs(allLogs), blocks: allBlocks };
  }

  private async query(
    body: Record<string, unknown>
  ): Promise<HypersyncResponse> {
    const url = this.url;
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

  private convertBlocks(blocks: HypersyncBlock[]): FetchedBlock[] {
    const result: FetchedBlock[] = [];

    for (const block of blocks) {
      if (
        block.number != null &&
        block.timestamp != null &&
        block.hash != null &&
        block.parent_hash != null
      ) {
        result.push({
          number: block.number,
          hash: block.hash,
          parentHash: block.parent_hash,
          timestamp: block.timestamp
        });
      }
    }

    return result;
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
