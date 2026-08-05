import { describe, expect, it } from 'bun:test';
import { getRangeHint } from './helpers';
import { CustomJsonRpcError } from './types';

describe('getRangeHint', () => {
  it('should return null on unknown errors', () => {
    const genericError = new Error('Generic error');

    const range = getRangeHint(genericError, {
      from: 1000,
      to: 2000
    });

    expect(range).toBe(null);
  });

  it('should return null on unknown errors', () => {
    const unknownError = new CustomJsonRpcError('Unknown error', -99999, {
      some: 'data'
    });

    expect(
      getRangeHint(unknownError, {
        from: 1000,
        to: 2000
      })
    ).toBe(null);
  });

  describe('Infura error (code: -32005)', () => {
    it('should return range from error data', () => {
      const infuraError = new CustomJsonRpcError(
        'Block range too large',
        -32005,
        {
          from: '0x3e8',
          to: '0x4cf'
        }
      );

      const result = getRangeHint(infuraError, {
        from: 1000,
        to: 2000
      });

      expect(result).toEqual({
        from: 1000,
        to: 1231
      });
    });

    it('should not return range if data is invalid', () => {
      const infuraError = new CustomJsonRpcError(
        'Block range too large',
        -32005,
        {}
      );

      const result = getRangeHint(infuraError, {
        from: 1000,
        to: 2000
      });

      expect(result).toEqual(null);
    });

    it('should not return range if data is missing to', () => {
      const infuraError = new CustomJsonRpcError(
        'Block range too large',
        -32005,
        {
          from: '0x3e8'
        }
      );

      const result = getRangeHint(infuraError, {
        from: 1000,
        to: 2000
      });

      expect(result).toEqual(null);
    });
  });

  describe('Ankr error (code: -32062)', () => {
    it('should return half the current range', () => {
      const ankrError = new CustomJsonRpcError(
        'Block range is too large',
        -32062,
        {}
      );

      const result = getRangeHint(ankrError, {
        from: 1000,
        to: 2000
      });

      expect(result).toEqual({
        from: 1000,
        to: 1500
      });
    });

    it('should round up', () => {
      const ankrError = new CustomJsonRpcError(
        'Block range is too large',
        -32062,
        {}
      );

      const result = getRangeHint(ankrError, {
        from: 1000,
        to: 1501
      });

      expect(result).toEqual({
        from: 1000,
        to: 1251
      });
    });
  });

  describe('Edge RPC error (code: -32012)', () => {
    it('should return half the current range', () => {
      const edgeError = new CustomJsonRpcError(
        'getLogs request exceeded max allowed range',
        -32012,
        {}
      );

      const result = getRangeHint(edgeError, {
        from: 1000,
        to: 2000
      });

      expect(result).toEqual({
        from: 1000,
        to: 1500
      });
    });

    it('should round up', () => {
      const edgeError = new CustomJsonRpcError(
        'getLogs request exceeded max allowed range',
        -32012,
        {}
      );

      const result = getRangeHint(edgeError, {
        from: 1000,
        to: 1501
      });

      expect(result).toEqual({
        from: 1000,
        to: 1251
      });
    });

    it('should return null when range can not be shrunk further', () => {
      const edgeError = new CustomJsonRpcError(
        'getLogs request exceeded max allowed range',
        -32012,
        {}
      );

      expect(
        getRangeHint(edgeError, {
          from: 1000,
          to: 1001
        })
      ).toEqual(null);

      expect(
        getRangeHint(edgeError, {
          from: 1000,
          to: 1000
        })
      ).toEqual(null);
    });
  });
});
