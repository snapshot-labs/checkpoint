import { describe, expect, it } from 'bun:test';
import { buildSchema, GraphQLObjectType } from 'graphql';
import {
  codegen,
  getBaseType,
  getInitialValue,
  getJSType,
  getTypeInfo
} from '../../src/codegen';
import { GqlEntityController } from '../../src/graphql/controller';
import Model from '../../src/orm/model';
import { extendSchema } from '../../src/utils/graphql';

const SCHEMA_SOURCE = `
scalar Id
scalar Text
scalar BigInt
scalar BigDecimal
scalar Unknown

type Space {
  id: String!
  name: String
  about: String
  controller: String!
  voting_delay: Int!
  proposal_threshold: BigInt!
  quorum: Float!
  strategies: [String]!
  strategies_nonnull: [String!]!
  proposals: [Proposal]! @derivedFrom(field: "space")
}

type Proposal {
  id: String!
  proposal_id: Int!
  space: Space!
  title: Text!
  scores_total: BigInt!
  active: Boolean!
  progress: BigDecimal!
}
`;

const schema = buildSchema(extendSchema(SCHEMA_SOURCE));
const space = schema.getType('Space') as GraphQLObjectType;
const proposal = schema.getType('Proposal') as GraphQLObjectType;
const spaceFields = space.getFields();
const proposalFields = proposal.getFields();

describe('getTypeInfo', () => {
  const simpleSchema = `scalar HugeDecimal
type Space {
  id: String!
  value: HugeDecimal
}
`;

  const customDecimalTypes = {
    HugeDecimal: {
      p: 30,
      d: 14
    }
  };

  const schema = buildSchema(extendSchema(simpleSchema));
  const space = schema.getType('Space') as GraphQLObjectType;
  const spaceFields = space.getFields();

  it('should throw when passed a wrapped type', () => {
    expect(() => getTypeInfo(spaceFields['id'].type)).toThrow();
  });

  it('should throw when passing unknown types', () => {
    expect(() => getTypeInfo(spaceFields['value'].type)).toThrow();
  });

  it('should handle non-default decimalTypes', () => {
    expect(getTypeInfo(spaceFields['value'].type, customDecimalTypes)).toEqual({
      type: 'string',
      initialValue: '0'
    });
  });
});

describe('getInitialValue', () => {
  it('should return null for nullable types', () => {
    expect(getInitialValue(spaceFields['name'].type)).toBeNull();
    expect(getInitialValue(spaceFields['about'].type)).toBeNull();
  });

  it('should return 0 for Int/Float types', () => {
    expect(getInitialValue(spaceFields['voting_delay'].type)).toBe(0);
    expect(getInitialValue(spaceFields['quorum'].type)).toBe(0);
  });

  it('should return 0n for BigInt types', () => {
    expect(getInitialValue(spaceFields['proposal_threshold'].type)).toBe(0n);
  });

  it('should return 0 string for BigDecimal types', () => {
    expect(getInitialValue(proposalFields['progress'].type)).toBe('0');
  });

  it('should return empty string for String/Text/Id types', () => {
    expect(getInitialValue(spaceFields['id'].type)).toBe('');
    expect(getInitialValue(spaceFields['controller'].type)).toBe('');
    expect(getInitialValue(proposalFields['title'].type)).toBe('');
  });

  it('should return false for Boolean types', () => {
    expect(getInitialValue(proposalFields['active'].type)).toBe(false);
  });

  it('should return stringified empty array for List types', () => {
    expect(getInitialValue(spaceFields['strategies'].type)).toEqual('[]');
    expect(getInitialValue(spaceFields['strategies_nonnull'].type)).toEqual(
      '[]'
    );
  });

  it('should return empty string for object types', () => {
    expect(getInitialValue(proposalFields['space'].type)).toBe('');
  });

  it('should return "0" for BigDecimal types', () => {
    expect(getInitialValue(proposalFields['progress'].type)).toBe('0');
  });
});

describe('getBaseType', () => {
  it('should return number for Int/Float types', () => {
    expect(getBaseType(spaceFields['voting_delay'].type)).toBe('number');
    expect(getBaseType(proposalFields['proposal_id'].type)).toBe('number');
  });

  it('should return string for String/Text/Id types', () => {
    expect(getBaseType(spaceFields['id'].type)).toBe('string');
    expect(getBaseType(spaceFields['name'].type)).toBe('string');
    expect(getBaseType(proposalFields['title'].type)).toBe('string');
  });

  it('should return string for Object types', () => {
    expect(getBaseType(proposalFields['space'].type)).toBe('string');
  });

  it('should return bigint for BigInt types', () => {
    expect(getBaseType(spaceFields['proposal_threshold'].type)).toBe('bigint');
  });

  it('should return boolean for Boolean types', () => {
    expect(getBaseType(proposalFields['active'].type)).toBe('boolean');
  });

  it('should return string for BigDecimal types', () => {
    expect(getBaseType(proposalFields['progress'].type)).toBe('string');
  });

  it('should return array type for List types', () => {
    expect(getBaseType(spaceFields['strategies'].type)).toBe('string[]');
    expect(getBaseType(spaceFields['strategies_nonnull'].type)).toBe(
      'string[]'
    );
  });

  it('should return string for BigDecimal types', () => {
    expect(getBaseType(proposalFields['progress'].type)).toBe('string');
  });
});

describe('getJSType', () => {
  it('should detect nullable types', () => {
    expect(getJSType(spaceFields['name'])).toEqual({
      isNullable: true,
      isList: false,
      baseType: 'string'
    });
  });

  it('should detect list types', () => {
    expect(getJSType(spaceFields['strategies'])).toEqual({
      isNullable: false,
      isList: true,
      baseType: 'string[]'
    });
  });
});

describe('codegen', () => {
  const overridesConfig = {};
  const extendedSchema = extendSchema(SCHEMA_SOURCE);
  const controller = new GqlEntityController(extendedSchema);

  it('should generate typescript code', () => {
    expect(
      codegen(controller, overridesConfig, 'typescript')
    ).toMatchSnapshot();
  });

  it('should generate javascript code', () => {
    expect(
      codegen(controller, overridesConfig, 'javascript')
    ).toMatchSnapshot();
  });
});

describe('generated models', () => {
  const extendedSchema = extendSchema(SCHEMA_SOURCE);
  const controller = new GqlEntityController(extendedSchema);

  const source = codegen(controller, {}, 'javascript')
    .replace(/^import .*\n\n/, '')
    .replace(/export /g, '');
  const { Space, Proposal } = new Function(
    'Model',
    `${source}\nreturn { Space, Proposal };`
  )(Model);

  describe('varchar length validation', () => {
    it('should reject id longer than 256 characters', () => {
      expect(() => new Space('x'.repeat(257), 'indexer')).toThrow(
        'Value for Space.id exceeds 256 characters'
      );
    });

    it('should reject values longer than 256 characters for String fields', () => {
      const space = new Space('space-1', 'indexer');

      expect(() => {
        space.controller = 'x'.repeat(257);
      }).toThrow('Value for Space.controller exceeds 256 characters');
    });

    it('should accept values up to 256 characters', () => {
      const space = new Space('space-1', 'indexer');
      space.controller = 'x'.repeat(256);

      expect(space.controller).toHaveLength(256);
    });

    it('should count characters, not UTF-16 code units', () => {
      const space = new Space('space-1', 'indexer');
      const emojis = '🙂'.repeat(200);
      space.controller = emojis;

      expect(space.controller).toBe(emojis);
    });

    it('should accept null for nullable String fields', () => {
      const space = new Space('space-1', 'indexer');
      space.name = null;

      expect(space.name).toBeNull();
    });

    it('should reject long values for nullable String fields', () => {
      const space = new Space('space-1', 'indexer');

      expect(() => {
        space.name = 'x'.repeat(257);
      }).toThrow('Value for Space.name exceeds 256 characters');
    });

    it('should reject long values for object reference fields', () => {
      const proposal = new Proposal('proposal-1', 'indexer');

      expect(() => {
        proposal.space = 'x'.repeat(257);
      }).toThrow('Value for Proposal.space exceeds 256 characters');
    });

    it('should not limit Text fields', () => {
      const proposal = new Proposal('proposal-1', 'indexer');
      const longText = 'x'.repeat(10000);
      proposal.title = longText;

      expect(proposal.title).toBe(longText);
    });

    it('should not limit list fields', () => {
      const space = new Space('space-1', 'indexer');
      const strategies = Array.from({ length: 100 }, (_, i) =>
        `strategy-${i}`.repeat(30)
      );
      space.strategies = strategies;

      expect(space.strategies).toEqual(strategies);
    });
  });

  describe('NUL character validation', () => {
    it('should reject id containing NUL character', () => {
      expect(() => new Space('space\u0000-1', 'indexer')).toThrow(
        'Value for Space.id contains a NUL character'
      );
    });

    it('should reject NUL character in String fields', () => {
      const space = new Space('space-1', 'indexer');

      expect(() => {
        space.controller = 'abc\u0000def';
      }).toThrow('Value for Space.controller contains a NUL character');
    });

    it('should reject NUL character in Text fields', () => {
      const proposal = new Proposal('proposal-1', 'indexer');

      expect(() => {
        proposal.title = 'abc\u0000def';
      }).toThrow('Value for Proposal.title contains a NUL character');
    });

    it('should reject NUL character in list elements', () => {
      const space = new Space('space-1', 'indexer');

      expect(() => {
        space.strategies = ['ok', 'bad\u0000'];
      }).toThrow('Value for Space.strategies contains a NUL character');
    });
  });
});
