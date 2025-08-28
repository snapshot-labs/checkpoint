import {
  factoryAbiCairo1,
  spaceAbi,
  spaceDeployedEvent,
  spaceDeployedEventCairo1,
  spaceFactoryAbi,
  voteCreatedEvent
} from './fixtures';
import { parseEvent } from '../../../../src/providers/starknet/utils';

describe('utils', () => {
  describe('parseEvent', () => {
    it('should parse event', () => {
      const output = parseEvent(spaceFactoryAbi, spaceDeployedEvent);

      expect(output).toMatchSnapshot();
    });

    it('should parse nested event', () => {
      const output = parseEvent(spaceAbi, voteCreatedEvent);

      expect(output).toMatchSnapshot();
    });

    it('should parse cairo 1 event', () => {
      const output = parseEvent(factoryAbiCairo1, spaceDeployedEventCairo1);

      expect(output).toMatchSnapshot();
    });
  });
});
