import { Log } from 'viem';
import { EvmProvider } from './provider';
import { Block } from './types';
import { CheckpointRecord } from '../../stores/checkpoints';
import { ContractSourceConfig } from '../../types';

type FetchedBlock = {
  number: number;
  hash: string;
  parentHash: string;
  timestamp: number;
};

type HypersyncResponse = {
  next_block: number;
  data: {
    blocks: {
      number?: number;
      timestamp?: number;
      hash?: string;
      parent_hash?: string;
    }[];
    logs: {
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
    }[];
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

export class HyperSyncEvmProvider extends EvmProvider {
  private readonly apiToken: string;
  private hypersyncUrl?: string;
  private blockCache = new Map<number, FetchedBlock>();

  constructor(
    params: ConstructorParameters<typeof EvmProvider>[0] & {
      apiToken: string;
    }
  ) {
    super(params);
    this.apiToken = params.apiToken;
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number
  ): Promise<CheckpointRecord[]> {
    const sources = this.instance.getCurrentSources(toBlock);
    this.blockCache.clear();

    const { logs, blocks } = await this.queryCheckpointsRange(
      fromBlock,
      toBlock,
      sources
    );

    for (const block of blocks) {
      this.blockCache.set(block.number, block);
    }

    for (const log of logs) {
      if (log.blockNumber === null) continue;

      if (!this.logsCache.has(log.blockNumber)) {
        this.logsCache.set(log.blockNumber, []);
      }

      this.logsCache.get(log.blockNumber)?.push(log);
    }

    return logs.map(log => ({
      blockNumber: Number(log.blockNumber),
      contractAddress: log.address
    }));
  }

  protected async fetchBlock(blockNumber: number): Promise<Block> {
    const cached = this.blockCache.get(blockNumber);
    if (cached) {
      this.blockCache.delete(blockNumber);
      return {
        number: BigInt(cached.number),
        hash: cached.hash,
        parentHash: cached.parentHash,
        timestamp: BigInt(cached.timestamp)
      } as Block;
    }

    return super.fetchBlock(blockNumber);
  }

  private async queryCheckpointsRange(
    fromBlock: number,
    toBlock: number,
    sources: ContractSourceConfig[]
  ): Promise<{ logs: Log[]; blocks: FetchedBlock[] }> {
    const allLogs: Log[] = [];
    const allBlocks: FetchedBlock[] = [];

    const addresses = sources.map(source => source.contract);
    const topic0 = sources.flatMap(source =>
      source.events.map(event => this.getEventHash(event.name))
    );

    let currentFrom = fromBlock;
    const exclusiveTo = toBlock + 1;

    while (currentFrom < exclusiveTo) {
      const response = await this.query({
        from_block: currentFrom,
        to_block: exclusiveTo,
        logs: [{ address: addresses, topics: [topic0] }],
        field_selection: FIELD_SELECTION
      });

      // NOTE: do not replace for/push with spread — spread causes stack overflow on large arrays
      for (const block of response.data.blocks) {
        if (
          block.number != null &&
          block.timestamp != null &&
          block.hash &&
          block.parent_hash
        ) {
          allBlocks.push({
            number: block.number,
            hash: block.hash,
            parentHash: block.parent_hash,
            timestamp: block.timestamp
          });
        }
      }

      for (const log of response.data.logs) {
        const topics = [
          log.topic0,
          log.topic1,
          log.topic2,
          log.topic3
        ].filter((t): t is string => !!t) as `0x${string}`[];

        allLogs.push({
          address: (log.address ?? '0x') as `0x${string}`,
          blockHash: (log.block_hash ?? null) as `0x${string}` | null,
          blockNumber:
            log.block_number != null ? BigInt(log.block_number) : null,
          data: (log.data ?? '0x') as `0x${string}`,
          logIndex: log.log_index ?? 0,
          transactionHash: (log.transaction_hash ?? null) as
            | `0x${string}`
            | null,
          transactionIndex: log.transaction_index ?? 0,
          removed: log.removed ?? false,
          topics
        } as Log);
      }

      if (response.next_block >= exclusiveTo) break;
      currentFrom = response.next_block;
    }

    return { logs: allLogs, blocks: allBlocks };
  }

  private async getHypersyncUrl(): Promise<string> {
    if (!this.hypersyncUrl) {
      const chainId = await this.getChainId();
      this.hypersyncUrl = `https://${chainId}.hypersync.xyz`;
    }
    return this.hypersyncUrl;
  }

  private async query(
    body: Record<string, unknown>
  ): Promise<HypersyncResponse> {
    const url = await this.getHypersyncUrl();

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
}
