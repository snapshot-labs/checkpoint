import objectHash from 'object-hash';
import {
  CheckpointConfig,
  ContractSourceConfig,
  ContractTemplate
} from '../types';

export const getContractsFromConfig = (config: CheckpointConfig): string[] => {
  return (config.sources || []).map(source => source.contract);
};

const getHashableProperties = (
  config: ContractTemplate | ContractSourceConfig
) => ({
  contract: 'contract' in config ? config.contract : undefined,
  start: 'start' in config ? config.start : undefined,
  events: (config.events || []).map(event => event.name)
});

export const getConfigChecksum = (config: CheckpointConfig): string => {
  const { tx_fn, global_events, sources, templates } = config;

  return objectHash(
    {
      tx_fn,
      global_events,
      sources: (sources || []).map(source => getHashableProperties(source)),
      templates: Object.fromEntries(
        Object.entries(templates || {}).map(([key, value]) => [
          key,
          getHashableProperties(value)
        ])
      )
    },
    {
      unorderedArrays: true
    }
  );
};
