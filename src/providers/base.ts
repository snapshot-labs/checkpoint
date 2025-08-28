import { CheckpointRecord } from '../stores/checkpoints';
import {
  CheckpointConfig,
  CheckpointOptions,
  ContractSourceConfig
} from '../types';
import { Logger } from '../utils/logger';

export type Instance = {
  config: CheckpointConfig;
  opts?: CheckpointOptions;
  getCurrentSources(blockNumber: number): ContractSourceConfig[];
  setBlockHash(blockNum: number, hash: string);
  setLastIndexedBlock(blockNum: number);
  insertCheckpoints(checkpoints: CheckpointRecord[]);
  getWriterHelpers(): {
    executeTemplate(
      template: string,
      config: { contract: string; start: number },
      persist?: boolean
    );
  };
};

export class BlockNotFoundError extends Error {
  constructor() {
    super('Block not found');
    this.name = 'BlockNotFoundError';
  }
}

export class ReorgDetectedError extends Error {
  constructor() {
    super('Reorg detected');
    this.name = 'ReorgDetectedError';
  }
}

export class BaseProvider {
  protected readonly instance: Instance;
  protected readonly log: Logger;
  protected readonly abis: Record<string, any> = {};

  constructor({
    instance,
    log,
    abis
  }: {
    instance: Instance;
    log: Logger;
    abis?: Record<string, any>;
  }) {
    this.instance = instance;
    this.log = log;
    if (abis) {
      this.abis = abis;
    }
  }

  formatAddresses(addresses: string[]): string[] {
    throw new Error(
      `formatAddresses method was not defined when formatting ${addresses.length} addresses`
    );
  }

  getNetworkIdentifier(): Promise<string> {
    throw new Error('getNetworkIdentifier method was not defined');
  }

  getLatestBlockNumber(): Promise<number> {
    throw new Error('getLatestBlockNumber method was not defined');
  }

  getBlockHash(blockNumber: number): Promise<string> {
    throw new Error(
      `getBlockHash method was not defined when getting block hash for block ${blockNumber}`
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  processBlock(blockNum: number, parentHash: string | null): Promise<number> {
    throw new Error(
      `processBlock method was not defined when fetching block ${blockNum}`
    );
  }

  processPool(blockNumber: number): Promise<void> {
    throw new Error(
      `processPool method was not defined when fetching pool for block ${blockNumber}`
    );
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number
  ): Promise<CheckpointRecord[]> {
    throw new Error(
      `getCheckpointsRange method was not defined when fetching events from ${fromBlock} to ${toBlock}`
    );
  }
}

export class BaseIndexer {
  protected provider?: BaseProvider;

  init({
    /* eslint-disable @typescript-eslint/no-unused-vars */
    instance,
    log,
    abis
    /* eslint-enable @typescript-eslint/no-unused-vars */
  }: {
    instance: Instance;
    log: Logger;
    abis?: Record<string, any>;
  }) {
    throw new Error('init method was not defined');
  }

  public getProvider() {
    if (!this.provider) {
      throw new Error('Provider not initialized');
    }

    return this.provider;
  }

  public getHandlers(): string[] {
    throw new Error('getHandlers method was not defined');
  }
}
