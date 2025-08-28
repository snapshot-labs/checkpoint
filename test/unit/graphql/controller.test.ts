import { GraphQLObjectType, GraphQLSchema, printSchema } from 'graphql';
import knex from 'knex';
import { GqlEntityController } from '../../../src/graphql/controller';

describe('GqlEntityController', () => {
  describe('generateQueryFields', () => {
    it('should work', () => {
      const controller = new GqlEntityController(`
type Vote {
  id: Int!
  name: String
  authenticators: [String]
}
  `);
      const queryFields = controller.generateQueryFields();
      const querySchema = new GraphQLObjectType({
        name: 'Query',
        fields: queryFields
      });

      const schema = printSchema(new GraphQLSchema({ query: querySchema }));
      expect(schema).toMatchSnapshot();
    });

    // list of error table tests
    describe.each([
      {
        reason: 'non null object id',
        schema: `type Vote { id: String }`
      },
      {
        reason: 'object id is not scalar type',
        schema: `type Vote { id: Participant! }\n\n type Participant { id: Int! }`
      },
      {
        reason: 'object id is not scalar type 2',
        schema: `type Participant { id: [Int]! }`
      }
    ])('should fail for $reason', ({ schema }) => {
      const controller = new GqlEntityController(schema);
      expect(() =>
        controller.generateQueryFields()
      ).toThrowErrorMatchingSnapshot();
    });
  });

  describe('createEntityStores', () => {
    const mockKnex = knex({
      client: 'sqlite3',
      connection: {
        filename: ':memory:'
      },
      useNullAsDefault: true
    });

    afterAll(async () => {
      await mockKnex.destroy();
    });

    it('should work', async () => {
      const controller = new GqlEntityController(`
scalar BigInt
scalar Decimal
scalar BigDecimal

type Vote {
  id: Int!
  name: String
  authenticators: [String]
  big_number: BigInt
  decimal: Decimal
  big_decimal: BigDecimal
}
  `);
      const { builder } = await controller.createEntityStores(mockKnex);

      expect(builder.toString()).toMatchSnapshot();
    });
  });

  describe('generateSampleQuery', () => {
    it('should return undefined when no entities are defined', () => {
      const controller = new GqlEntityController('scalar Text');

      expect(controller.generateSampleQuery()).toBeUndefined();
    });

    it.each([
      {
        case: 'first and only entity',
        schema: `
type Vote {
  id: Int!
  name: String
  created_at: Int!
}
        `
      },
      {
        case: 'nested objects',
        // Checkpoint doesn't support relationship among entities,
        // but this just tests to ensure the sample query works for it.
        schema: `
type Vote {
  id: Int!
  name: String
  poster: Poster
  created_at: Int!
}

type Poster {
  id: Int!
  name: String!
  venue: Venue!
}

type Venue {
  id: Int!
  location: String!
}
        `
      }
    ])('should return correct query sample for $case', ({ schema }) => {
      const controller = new GqlEntityController(schema);

      expect(controller.generateSampleQuery()).not.toBeUndefined();
      expect(controller.generateSampleQuery()).toMatchSnapshot();
    });

    it('should generate query schema', () => {
      const schema = `
        type Vote {
          id: Int!
          name: String
          authenticators: [String]
        }`;

      const controller = new GqlEntityController(schema);
      const generatedSchema = controller.generateSchema();

      expect(printSchema(generatedSchema)).toMatchSnapshot();
    });
  });
});
