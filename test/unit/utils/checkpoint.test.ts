import { describe, expect, it } from 'bun:test';
import { getContractsFromConfig } from '../../../src/utils/checkpoint';
import { validCheckpointConfig } from '../../fixtures/checkpointConfig.fixture';

describe('getContractsFromConfig', () => {
  it('should work', () => {
    expect(getContractsFromConfig(validCheckpointConfig)).toMatchSnapshot();
  });
});
