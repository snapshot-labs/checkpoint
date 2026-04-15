import { Knex } from 'knex';
import { EntityWriteBuffer } from './orm/buffer';

function createRegister() {
  let knexInstance: Knex | null = null;
  const currentBlocks = new Map<string, bigint>();
  const buffers = new Map<string, EntityWriteBuffer>();

  return {
    getCurrentBlock(indexerName: string) {
      return currentBlocks.get(indexerName) || 0n;
    },
    setCurrentBlock(indexerName: string, block: bigint) {
      currentBlocks.set(indexerName, block);
    },
    getKnex() {
      if (!knexInstance) {
        throw new Error('Knex is not initialized yet.');
      }

      return knexInstance;
    },
    setKnex(knex: Knex) {
      knexInstance = knex;
    },
    getBuffer(indexerName: string): EntityWriteBuffer {
      const buffer = buffers.get(indexerName);
      if (!buffer) {
        throw new Error(
          `Write buffer for indexer '${indexerName}' is not initialized.`
        );
      }
      return buffer;
    },
    setBuffer(indexerName: string, buffer: EntityWriteBuffer) {
      buffers.set(indexerName, buffer);
    }
  };
}

export const register = createRegister();
