import { register } from '../register';

export default class Model {
  private tableName: string;
  private indexerName: string;
  private values = new Map<string, any>();
  private exists = false;

  constructor(tableName: string, indexerName: string) {
    this.tableName = tableName;
    this.indexerName = indexerName;
  }

  setExists() {
    this.exists = true;
  }

  initialSet(key: string, value: any) {
    this.values.set(key, value);
  }

  get(key: string): any {
    return this.values.get(key) ?? null;
  }

  set(key: string, value: any) {
    this.values.set(key, value);
  }

  static async _loadEntity(
    tableName: string,
    id: string | number,
    indexerName: string
  ): Promise<Record<string, any> | null> {
    return register
      .getEntityBuffer(indexerName)
      .load({ tableName, id: String(id) });
  }

  async save() {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { uid, ...values } = Object.fromEntries(this.values.entries());

    register.getEntityBuffer(this.indexerName).save({
      tableName: this.tableName,
      id: String(this.get('id')),
      values,
      block: register.getCurrentBlock(this.indexerName),
      hasDatabaseRow: this.exists
    });

    this.exists = true;
  }

  async delete() {
    if (!this.exists) return;

    register.getEntityBuffer(this.indexerName).delete({
      tableName: this.tableName,
      id: String(this.get('id')),
      block: register.getCurrentBlock(this.indexerName),
      hasDatabaseRow: this.exists
    });
  }
}
