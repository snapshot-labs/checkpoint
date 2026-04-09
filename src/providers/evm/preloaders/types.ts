import { Log } from 'viem';
import { CheckpointRecord } from '../../../stores/checkpoints';
import { ContractSourceConfig } from '../../../types';

export type FetchedBlock = {
  number: number;
  hash: string;
  parentHash: string;
  timestamp: number;
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
