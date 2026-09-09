import { Logger } from '../../utils/logger';
import { BaseIndexer, Instance, RpcSelector } from '../base';
import { EvmProvider } from './provider';
import { Writer } from './types';

export class EvmIndexer extends BaseIndexer {
  private writers: Record<string, Writer>;
  private options: { rpcSelector?: RpcSelector };

  constructor(
    writers: Record<string, Writer>,
    options: { rpcSelector?: RpcSelector } = {}
  ) {
    super();
    this.writers = writers;
    this.options = options;
  }

  init({
    instance,
    log,
    abis
  }: {
    instance: Instance;
    log: Logger;
    abis?: Record<string, any>;
  }) {
    this.provider = new EvmProvider({
      instance,
      log,
      abis,
      writers: this.writers,
      rpcSelector: this.options.rpcSelector
    });
  }

  public getHandlers(): string[] {
    return Object.keys(this.writers);
  }
}
