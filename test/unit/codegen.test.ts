import { buildSchema, GraphQLObjectType } from 'graphql';
import {
  codegen,
  getBaseType,
  getInitialValue,
  getJSType,
  getTypeInfo
} from '../../src/codegen';
import { GqlEntityController } from '../../src/graphql/controller';
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

  it('should return 0 for Int/Float/BigInt types', () => {
    expect(getInitialValue(spaceFields['voting_delay'].type)).toBe(0);
    expect(getInitialValue(spaceFields['proposal_threshold'].type)).toBe(0);
    expect(getInitialValue(spaceFields['quorum'].type)).toBe(0);
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
