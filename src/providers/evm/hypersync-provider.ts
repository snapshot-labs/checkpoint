import { Log } from 'viem';
import { EvmProvider } from './provider';
import { Block } from './types';
import { CheckpointRecord } from '../../stores/checkpoints';
import { ContractSourceConfig } from '../../types';
import { sleep } from '../../utils/helpers';

type FetchedBlock = {
  number: number;
  hash: string;
  parentHash: string;
  timestamp: number;
};

type HyperSyncBlock = {
  number: number;
  timestamp: number;
  hash: string;
  parent_hash: string;
};

type HyperSyncLog = {
  block_number: number;
  log_index: number;
  transaction_index: number;
  transaction_hash: string;
  block_hash: string;
  address: string;
  data: string;
  topic0: string | null;
  topic1: string | null;
  topic2: string | null;
  topic3: string | null;
  removed: boolean;
};

type HyperSyncResponse = {
  next_block: number;
  data: {
    blocks?: HyperSyncBlock[];
    logs?: HyperSyncLog[];
  }[];
};

const PRELOAD_RANGE = 1000000;

const MAX_RETRIES = 8;
const BASE_RETRY_DELAY = 1000;
const MAX_RETRY_DELAY = 30000;
const RATE_LIMIT_WINDOW = 60000;

const REQUESTS_PER_MINUTE = 30;

/**
 * Spaces out requests so that no more than `requestsPerMinute` are issued in
 * any rolling minute. A single instance is shared across all HyperSync
 * providers in the process because the rate limit is enforced globally across
 * all networks.
 */
class RateLimiter {
  private readonly minInterval: number;
  private nextSlot = 0;

  constructor(requestsPerMinute: number) {
    this.minInterval = Math.ceil(60000 / requestsPerMinute);
  }

  async wait(): Promise<void> {
    const now = Date.now();
    const slot = Math.max(now, this.nextSlot);
    this.nextSlot = slot + this.minInterval;

    const delay = slot - now;
    if (delay > 0) await sleep(delay);
  }

  pauseFor(ms: number) {
    this.nextSlot = Math.max(this.nextSlot, Date.now() + ms);
  }
}

const sharedRateLimiter = new RateLimiter(REQUESTS_PER_MINUTE);

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
  private hyperSyncUrl?: string;
  private blockCache = new Map<number, FetchedBlock>();

  constructor(
    params: ConstructorParameters<typeof EvmProvider>[0] & {
      apiToken: string;
    }
  ) {
    super(params);
    this.apiToken = params.apiToken;
  }

  getPreloadRange(): number {
    return PRELOAD_RANGE;
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

      for (const chunk of response.data) {
        // NOTE: do not replace for/push with spread — spread causes stack overflow on large arrays
        for (const block of chunk.blocks ?? []) {
          allBlocks.push({
            number: block.number,
            hash: block.hash,
            parentHash: block.parent_hash,
            timestamp: block.timestamp
          });
        }

        for (const log of chunk.logs ?? []) {
          const topics = [
            log.topic0,
            log.topic1,
            log.topic2,
            log.topic3
          ].filter((t): t is string => !!t) as `0x${string}`[];

          allLogs.push({
            address: log.address as `0x${string}`,
            blockHash: log.block_hash as `0x${string}`,
            blockNumber: BigInt(log.block_number),
            data: log.data as `0x${string}`,
            logIndex: log.log_index,
            transactionHash: log.transaction_hash as `0x${string}`,
            transactionIndex: log.transaction_index,
            removed: log.removed,
            topics
          } as Log);
        }
      }

      if (response.next_block >= exclusiveTo) break;
      currentFrom = response.next_block;
    }

    return { logs: allLogs, blocks: allBlocks };
  }

  private async getHyperSyncUrl(): Promise<string> {
    if (!this.hyperSyncUrl) {
      const chainId = await this.getChainId();
      this.hyperSyncUrl = `https://${chainId}.hypersync.xyz`;
    }
    return this.hyperSyncUrl;
  }

  private getResetSeconds(res: Response): number | null {
    const reset = res.headers.get('x-ratelimit-reset');
    if (reset === null) return null;

    const seconds = Number(reset);
    return Number.isFinite(seconds) && seconds > 0 ? seconds : null;
  }

  private getRetryDelay(res: Response, attempt: number): number {
    const resetSeconds = this.getResetSeconds(res);
    if (resetSeconds !== null) {
      return Math.min((resetSeconds + 1) * 1000, RATE_LIMIT_WINDOW);
    }

    return Math.min(BASE_RETRY_DELAY * 2 ** attempt, MAX_RETRY_DELAY);
  }

  private async query(
    body: Record<string, unknown>
  ): Promise<HyperSyncResponse> {
    const url = await this.getHyperSyncUrl();

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      await sharedRateLimiter.wait();

      const res = await fetch(`${url}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.apiToken}`
        },
        body: JSON.stringify(body)
      });

      if (res.ok) {
        return res.json();
      }

      const retryable = res.status === 429 || res.status >= 500;
      if (!retryable || attempt === MAX_RETRIES) {
        throw new Error(`HyperSync query failed: ${res.statusText}`);
      }

      const delay = this.getRetryDelay(res, attempt);
      if (res.status === 429) {
        sharedRateLimiter.pauseFor(delay);
      }

      await sleep(delay);
    }

    throw new Error('HyperSync query failed: retries exhausted');
  }
}
