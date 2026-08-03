import {
  GraphQLField,
  GraphQLFloat,
  GraphQLID,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLString,
  GraphQLType,
  isListType
} from 'graphql';
import {
  DecimalTypes,
  DEFAULT_DECIMAL_TYPES,
  GqlEntityController
} from './graphql/controller';
import { OverridesConfig } from './types';
import { getTableName } from './utils/database';
import { getComputedDirective, getDerivedFromDirective } from './utils/graphql';

type TypeInfo = {
  type: string;
  initialValue: any;
};

export const getTypeInfo = (
  type: GraphQLType,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
): TypeInfo => {
  if (type instanceof GraphQLNonNull) {
    throw new Error('Type must raw type');
  }

  switch (type) {
    case GraphQLInt:
    case GraphQLFloat:
      return { type: 'number', initialValue: 0 };
    case GraphQLString:
    case GraphQLID:
      return { type: 'string', initialValue: '' };
  }

  if (type instanceof GraphQLScalarType) {
    switch (type.name) {
      case 'BigInt':
        return { type: 'bigint', initialValue: 0n };
      case 'Boolean':
        return { type: 'boolean', initialValue: false };
      case 'Text':
        return { type: 'string', initialValue: '' };
    }

    if (type.name in decimalTypes) {
      return { type: 'string', initialValue: '0' };
    }
  }

  if (type instanceof GraphQLObjectType) {
    return { type: 'string', initialValue: '' };
  }

  if (type instanceof GraphQLList) {
    const nonNullNestedType =
      type.ofType instanceof GraphQLNonNull ? type.ofType.ofType : type.ofType;

    return {
      type: `${getTypeInfo(nonNullNestedType, decimalTypes).type}[]`,
      initialValue: '[]'
    };
  }

  throw new Error('Unknown type');
};

export const getInitialValue = (
  type: GraphQLType,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
) => {
  if (!(type instanceof GraphQLNonNull)) {
    return null;
  }

  return getTypeInfo(type.ofType, decimalTypes).initialValue;
};

export const getBaseType = (
  type: GraphQLType,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
) => {
  const nonNullType = type instanceof GraphQLNonNull ? type.ofType : type;

  return getTypeInfo(nonNullType, decimalTypes).type;
};

export const getJSType = (
  field: GraphQLField<any, any>,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
) => {
  const nonNullType =
    field.type instanceof GraphQLNonNull ? field.type.ofType : field.type;
  const isNullable = !(field.type instanceof GraphQLNonNull);
  const isList = nonNullType instanceof GraphQLList;
  const baseType = getBaseType(nonNullType, decimalTypes);

  return { isNullable, isList, baseType };
};

/** Maximum length of varchar columns created for String/ID fields. */
const VARCHAR_LENGTH = 256;

const isVarcharField = (type: GraphQLType): boolean => {
  const nonNullType = type instanceof GraphQLNonNull ? type.ofType : type;

  if (nonNullType === GraphQLString || nonNullType === GraphQLID) return true;

  if (nonNullType instanceof GraphQLObjectType) {
    const idField = nonNullType.getFields()['id'];

    return (
      idField !== undefined &&
      idField.type instanceof GraphQLNonNull &&
      idField.type.ofType instanceof GraphQLScalarType &&
      ['String', 'ID'].includes(idField.type.ofType.name)
    );
  }

  return false;
};

const isPersistedField = (field: GraphQLField<any, any>) => {
  if (getComputedDirective(field)) return false;

  const fieldType =
    field.type instanceof GraphQLNonNull ? field.type.ofType : field.type;

  return !(
    isListType(fieldType) &&
    fieldType.ofType instanceof GraphQLObjectType &&
    getDerivedFromDirective(field)
  );
};

export const codegen = (
  controller: GqlEntityController,
  config: OverridesConfig,
  format: 'typescript' | 'javascript'
) => {
  const decimalTypes = config.decimal_types || DEFAULT_DECIMAL_TYPES;

  const preamble = `import { Model } from '@snapshot-labs/checkpoint';\n\n`;

  let contents = `${preamble}`;

  controller.schemaObjects.forEach((type, i, arr) => {
    const modelName = type.name;

    const typeFields = controller.getTypeFields(type);
    const persistedFields = typeFields.filter(isPersistedField);
    const idField = typeFields.find(field => field.name === 'id');
    const idType = idField ? getJSType(idField, decimalTypes) : null;

    if (
      !idType ||
      !['string', 'number'].includes(idType.baseType) ||
      idType.isNullable ||
      idType.isList
    ) {
      throw new Error(
        `Model ${modelName} must have an id field of type string or number`
      );
    }

    contents += `export class ${modelName} extends Model {\n`;
    contents += `  static tableName = '${getTableName(modelName.toLowerCase())}';\n\n`;
    contents += `  static fieldNames = [${persistedFields.map(field => `'${field.name}'`).join(', ')}];\n\n`;

    contents +=
      format === 'javascript'
        ? `  constructor(id, indexerName) {\n`
        : `  constructor(id: ${idType.baseType}, indexerName: string) {\n`;
    contents += `    super(${modelName}.tableName, indexerName);\n\n`;
    if (idField && isVarcharField(idField.type)) {
      contents += `    if ([...id].length > ${VARCHAR_LENGTH}) {\n`;
      contents += `      throw new Error('Value for ${modelName}.id exceeds ${VARCHAR_LENGTH} characters');\n`;
      contents += `    }\n\n`;
    }
    if (idType.baseType === 'string') {
      contents += `    if (id.includes('\\u0000')) {\n`;
      contents += `      throw new Error('Value for ${modelName}.id contains a NUL character');\n`;
      contents += `    }\n\n`;
    }
    persistedFields.forEach(field => {
      const rawInitialValue = getInitialValue(field.type, decimalTypes);
      const initialValue =
        field.name === 'id'
          ? 'id'
          : typeof rawInitialValue === 'bigint'
            ? `${rawInitialValue}n`
            : JSON.stringify(rawInitialValue);
      contents += `    this.initialSet('${field.name}', ${initialValue});\n`;
    });
    contents += `  }\n\n`;

    contents +=
      format === 'javascript'
        ? `  static async loadEntity(id, indexerName) {\n`
        : `  static async loadEntity(id: ${idType.baseType}, indexerName: string): Promise<${modelName} | null> {\n`;
    contents += `    const entity = await super._loadEntity(${modelName}.tableName, id, indexerName);\n`;
    contents += `    if (!entity) return null;\n\n`;
    contents += `    const model = new ${modelName}(id, indexerName);\n`;
    contents += `    model.setExists();\n\n`;
    contents += `    for (const key of ${modelName}.fieldNames) {\n`;
    contents += `      const value = entity[key] !== null && typeof entity[key] === 'object'\n`;
    contents += `        ? JSON.stringify(entity[key])\n`;
    contents += `        : entity[key];\n`;
    contents += `      model.set(key, value);\n`;
    contents += `    }\n\n`;
    contents += `    return model;\n`;
    contents += `  }\n\n`;

    persistedFields.forEach(field => {
      const { isNullable, isList, baseType } = getJSType(field, decimalTypes);
      const typeAnnotation = isNullable ? `${baseType} | null` : baseType;
      const isBigInt = baseType === 'bigint';

      contents +=
        format === 'javascript'
          ? `  get ${field.name}() {\n`
          : `  get ${field.name}(): ${typeAnnotation} {\n`;
      if (isBigInt && isNullable) {
        contents += `    const value = this.get('${field.name}');\n`;
        contents += `    return value === null ? null : BigInt(value);\n`;
      } else {
        let getterExpression = `this.get('${field.name}')`;
        if (isList) getterExpression = `JSON.parse(${getterExpression})`;
        if (isBigInt) getterExpression = `BigInt(${getterExpression})`;
        contents += `    return ${getterExpression};\n`;
      }
      contents += `  }\n\n`;

      const setterAnnotation = isBigInt
        ? `bigint | number | string${isNullable ? ' | null' : ''}`
        : typeAnnotation;
      let setterExpression = 'value';
      if (isList) setterExpression = 'JSON.stringify(value)';
      if (isBigInt) {
        setterExpression = isNullable
          ? 'value === null ? null : BigInt(value)'
          : 'BigInt(value)';
      }

      contents +=
        format === 'javascript'
          ? `  set ${field.name}(value) {\n`
          : `  set ${field.name}(value: ${setterAnnotation}) {\n`;
      if (isVarcharField(field.type)) {
        const lengthCheck = `[...value].length > ${VARCHAR_LENGTH}`;
        contents += isNullable
          ? `    if (value !== null && ${lengthCheck}) {\n`
          : `    if (${lengthCheck}) {\n`;
        contents += `      throw new Error('Value for ${modelName}.${field.name} exceeds ${VARCHAR_LENGTH} characters');\n`;
        contents += `    }\n\n`;
      }
      if (baseType === 'string' || baseType === 'string[]') {
        const nulCheck = isList
          ? `value.some(item => typeof item === 'string' && item.includes('\\u0000'))`
          : `value.includes('\\u0000')`;
        contents += isNullable
          ? `    if (value !== null && ${nulCheck}) {\n`
          : `    if (${nulCheck}) {\n`;
        contents += `      throw new Error('Value for ${modelName}.${field.name} contains a NUL character');\n`;
        contents += `    }\n\n`;
      }
      contents += `    this.set('${field.name}', ${setterExpression});\n`;
      contents += `  }\n\n`;
    });

    contents = contents.slice(0, -1);
    contents += i === arr.length - 1 ? '}\n' : '}\n\n';
  });

  return contents;
};
