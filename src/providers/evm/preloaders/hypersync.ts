import { Log } from 'viem';
import { FetchedBlock, Preloader } from './types';
import { ContractSourceConfig } from '../../../types';
import { chunk } from '../../../utils/helpers';

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
  ): Promise<{ logs: Log[]; blocks: FetchedBlock[] }> {
    const allLogs: Log[] = [];
    const allBlocks: FetchedBlock[] = [];

    for (const sourceChunk of chunk(sources, 20)) {
      const addresses = sourceChunk.map(source => source.contract);
      const topic0 = sourceChunk.flatMap(source =>
        source.events.map(event => getEventHash(event.name))
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
    }

    return { logs: allLogs, blocks: allBlocks };
  }

  private async query(
    body: Record<string, unknown>
  ): Promise<HypersyncResponse> {
    const res = await fetch(`${this.url}/query`, {
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
