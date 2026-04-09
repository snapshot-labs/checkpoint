import { Log } from 'viem';
import { CheckpointRecord } from '../../../stores/checkpoints';
import { ContractSourceConfig } from '../../../types';
import { FetchedBlock, Preloader } from './types';

export class RpcPreloader implements Preloader {
  private readonly getLogs: (
    fromBlock: number,
    toBlock: number,
    address: string | string[],
    topics?: (string | string[])[]
  ) => Promise<Log[]>;

  constructor(
    getLogs: (
      fromBlock: number,
      toBlock: number,
      address: string | string[],
      topics?: (string | string[])[]
    ) => Promise<Log[]>
  ) {
    this.getLogs = getLogs;
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number,
    sources: ContractSourceConfig[],
    getEventHash: (name: string) => string
  ): Promise<{
    checkpoints: CheckpointRecord[];
    logs: Log[];
    blocks: FetchedBlock[];
  }> {
    const chunks: ContractSourceConfig[][] = [];
    for (let i = 0; i < sources.length; i += 20) {
      chunks.push(sources.slice(i, i + 20));
    }

    let logs: Log[] = [];
    for (const chunk of chunks) {
      const address = chunk.map(source => source.contract);
      const topics = chunk.flatMap(source =>
        source.events.map(event => getEventHash(event.name))
      );

      const chunkLogs = await this.getLogs(fromBlock, toBlock, address, [
        topics
      ]);
      logs = logs.concat(chunkLogs);
    }

    const checkpoints = logs.map(log => ({
      blockNumber: Number(log.blockNumber),
      contractAddress: log.address
    }));

    return { checkpoints, logs, blocks: [] };
  }
}
