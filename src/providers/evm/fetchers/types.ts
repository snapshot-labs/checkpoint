import { Log } from 'viem';
import { CheckpointRecord } from '../../../stores/checkpoints';
import { ContractSourceConfig } from '../../../types';

export type FetchedBlock = {
  number: number;
  hash: string;
  parentHash: string;
  timestamp: number;
};

export type BlockFetcher = {
  getChainId(): Promise<number>;
  getLatestBlockNumber(): Promise<number>;
  getBlock(blockNumber: number): Promise<FetchedBlock>;
  getBlockHash(blockNumber: number): Promise<string>;
  getLogs(
    fromBlock: number,
    toBlock: number,
    address: string | string[],
    topics?: (string | string[])[]
  ): Promise<Log[]>;
  getLogsByBlockHash(blockHash: string): Promise<Log[]>;
  getCheckpointsRange(
    fromBlock: number,
    toBlock: number,
    sources: ContractSourceConfig[],
    getEventHash: (name: string) => string
  ): Promise<{ checkpoints: CheckpointRecord[]; logs: Log[] }>;
};

export type Preloader = {
  getCheckpointsRange(
    fromBlock: number,
    toBlock: number,
    sources: ContractSourceConfig[],
    getEventHash: (name: string) => string
  ): Promise<{
    checkpoints: CheckpointRecord[];
    logs: Log[];
    blocks: FetchedBlock[];
  }>;
};
