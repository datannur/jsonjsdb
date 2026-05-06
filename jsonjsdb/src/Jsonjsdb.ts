import DBrowser from './DBrowser'
import Loader from './Loader'
import IntegrityChecker from './IntegrityChecker'
import type {
  JsonjsdbConfig,
  PartialJsonjsdbConfig,
  IntegrityResult,
  Schema,
  DatabaseMetadata,
  DatabaseRow,
  TableCollection,
} from './types'

type InitOption = {
  filter?: {
    entity?: string
    variable?: string
    values?: string[]
  }
  aliases?: Array<{ table: string; alias: string }>
  useCache?: boolean
  version?: number | string
  limit?: number
  validIdChars?: string
}

type LoadOption = {
  useCache?: boolean
  version?: number | string
  shouldStandardizeIds?: boolean
  shouldTransformKeys?: boolean
}

type ForeignTableObj = {
  [tableName: string]: string | number | { id: string | number } | undefined
}

export default class Jsonjsdb<
  TEntityTypeMap extends Record<string, DatabaseRow> = Record<
    string,
    DatabaseRow
  >,
> {
  defaultConfig: JsonjsdbConfig
  config!: JsonjsdbConfig
  browser: DBrowser
  loader: Loader
  integrityChecker: IntegrityChecker
  tables!: TableCollection<TEntityTypeMap>
  metadata!: DatabaseMetadata
  private idSuffix = 'Id'
  private usageCache: Partial<Record<keyof TEntityTypeMap, boolean>> = {}
  private recursiveUsageCache: Partial<Record<keyof TEntityTypeMap, boolean>> =
    {}

  constructor(config?: string | PartialJsonjsdbConfig) {
    this.defaultConfig = {
      path: 'db',
      dbKey: false,
      browserKey: false,
      appName: 'jsonjsdb',
      useCache: false,
      useEncryption: false,
      validIdChars: 'a-zA-Z0-9_, -',
    }

    let processedConfig: PartialJsonjsdbConfig = {}
    if (typeof config === 'string') {
      const htmlConfig = this.getHtmlConfig(config)
      processedConfig = htmlConfig || {}
    } else if (config) {
      processedConfig = config
    }

    this.setConfig(processedConfig)
    this.browser = new DBrowser(
      this.config.browserKey as string,
      this.config.appName,
      this.config.useEncryption,
    )
    this.loader = new Loader(this.browser, this.config)
    this.integrityChecker = new IntegrityChecker()
  }
  private getHtmlConfig(
    id = '#jsonjsdb-config',
  ): PartialJsonjsdbConfig | false {
    const configElement = document.querySelector(id) as HTMLElement
    if (!configElement) return false

    const { dataset } = configElement
    const config: PartialJsonjsdbConfig = {}

    if (dataset.path) config.path = dataset.path
    if (dataset.dbKey)
      config.dbKey = dataset.dbKey === 'false' ? false : dataset.dbKey
    if (dataset.browserKey)
      config.browserKey =
        dataset.browserKey === 'false' ? false : dataset.browserKey
    if (dataset.appName) config.appName = dataset.appName
    if (dataset.useCache) config.useCache = dataset.useCache === 'true'
    if (dataset.useEncryption)
      config.useEncryption = dataset.useEncryption === 'true'
    if (dataset.validIdChars) config.validIdChars = dataset.validIdChars

    return config
  }
  private setConfig(config: PartialJsonjsdbConfig): void {
    this.config = { ...this.defaultConfig, ...config }

    if (window?.location.protocol.startsWith('http')) {
      this.config.useCache = false
    }
    if (this.config.dbKey) {
      this.config.path += '/' + this.config.dbKey
    }
  }

  async init(option: InitOption = {}): Promise<Jsonjsdb<TEntityTypeMap>> {
    this.tables = (await this.loader.load(
      this.config.path,
      this.config.useCache,
      option,
    )) as TableCollection<TEntityTypeMap>
    this.metadata = this.loader.metadata

    this.computeUsage()
    return this
  }
  async load(
    filePath: string,
    name: string,
    option: boolean | LoadOption = true,
  ): Promise<unknown[]> {
    const loadOption =
      typeof option === 'boolean' ? { shouldStandardizeIds: option } : option

    filePath = this.config.path + '/' + filePath
    const data = await this.loader.loadJsonjs(filePath, name, {
      useCache: loadOption.useCache ?? false,
      version: loadOption.version ?? Date.now(),
      shouldStandardizeIds: loadOption.shouldStandardizeIds,
      shouldTransformKeys: loadOption.shouldTransformKeys,
    })
    return data
  }

  get<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
  ): TEntityTypeMap[K] | undefined {
    try {
      const tableData = this.tables[table]
      if (!Array.isArray(tableData)) return undefined

      const indexValue = this.metadata.index[table].id[id]
      if (typeof indexValue !== 'number') return undefined

      const result = tableData[indexValue]
      if (!result) {
        console.error(`table ${table}, id not found: ${id}`)
      }
      return result
    } catch {
      if (!this.tables[table]) {
        console.error(`table ${table} not found`)
      } else if (!this.metadata.index[table]) {
        console.error(`table ${table} not found in index`)
      } else if (!('id' in this.metadata.index[table])) {
        console.error(`table ${table}, props "id" not found in index`)
      } else {
        console.error(`error not handled`)
      }
      return undefined
    }
  }
  getAll<K extends keyof TEntityTypeMap>(
    table: K & string,
    foreignTableObj?: ForeignTableObj,
    option: InitOption = {},
  ): TEntityTypeMap[K][] {
    const tableData = this.tables[table]
    if (!Array.isArray(tableData)) return []

    if (!foreignTableObj) {
      if (option.limit) {
        return tableData.slice(0, option.limit)
      }
      return tableData
    }

    const foreignTableObjStart = Object.entries(foreignTableObj)[0]
    const foreignTable = foreignTableObjStart[0]
    let foreignValue: string | number | { id: string | number } | undefined =
      foreignTableObjStart[1]

    if (foreignValue === undefined) return []

    if (
      typeof foreignValue === 'object' &&
      foreignValue !== null &&
      'id' in foreignValue
    ) {
      foreignValue = foreignValue.id
    } else {
      foreignValue = foreignValue as string | number
    }

    let foreignKey = foreignTable + this.idSuffix
    if (foreignTable === table) foreignKey = 'parent' + this.idSuffix

    const indexAll = this.metadata.index[table][foreignKey]
    if (!indexAll || !(foreignValue in indexAll)) return []
    const indexes = indexAll[foreignValue]

    if (!Array.isArray(indexes)) {
      if (typeof indexes !== 'number' || !tableData[indexes]) {
        console.error('getAll() table', table, 'has an index undefined')
        return []
      }
      return [tableData[indexes]]
    }

    const variables = []
    for (const index of indexes) {
      if (option.limit && variables.length >= option.limit) break
      if (!tableData[index]) {
        console.error('getAll() table', table, 'has an index undefined')
        continue
      }
      variables.push(tableData[index])
    }
    return variables
  }
  getAllChilds<K extends keyof TEntityTypeMap>(
    table: K & string,
    itemId: string | number,
  ): TEntityTypeMap[K][] {
    const tableData = this.tables[table]
    if (!Array.isArray(tableData)) return []

    let all: TEntityTypeMap[K][] = []
    if (!itemId) {
      console.error('getAllChilds()', table, 'id', itemId)
      return all
    }
    const childs = this.getAll(table, { [table]: itemId })
    all = all.concat(childs)
    for (const child of childs) {
      const childRow = child
      if (itemId === childRow.id) {
        const msg = 'infinite loop for id'
        console.error('getAllChilds()', table, msg, itemId)
        return all
      }
      const newChilds = this.getAllChilds(table, childRow.id as string | number)
      all = all.concat(newChilds)
    }
    return all
  }
  foreach<K extends keyof TEntityTypeMap>(
    table: K & string,
    callback: (row: TEntityTypeMap[K]) => void,
  ): void {
    const rows = this.getAll(table)
    for (const row of rows) callback(row)
  }
  exists<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
  ): boolean {
    if (!this.tables[table]) return false
    if (!this.metadata.index[table]) return false
    if (!this.metadata.index[table].id) return false
    return id in this.metadata.index[table].id
  }
  getConfig(id: string | number): string | number | undefined {
    const configTable = this.tables['config']
    if (!Array.isArray(configTable)) return undefined

    const index = this.metadata.index['config'].id
    if (!index) return undefined
    if (!(id in index)) return undefined

    const indexValue = index[id]
    if (typeof indexValue !== 'number') return undefined

    const row = configTable[indexValue]
    return row['value'] as string | number
  }
  countRelated<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
    relatedTable: string,
  ): number {
    if (!(relatedTable in this.metadata.index)) return 0
    const index = this.metadata.index[relatedTable][table + this.idSuffix]
    if (!index) return 0
    if (!(id in index)) return 0
    const indexValue = index[id]
    if (!Array.isArray(indexValue)) return 1
    return indexValue.length
  }
  getParents<K extends keyof TEntityTypeMap>(
    from: K & string,
    id: string | number,
  ): TEntityTypeMap[K][] {
    if (!id || id === null) return []
    let parent = this.get(from, id)
    if (!parent) return []

    const parents: TEntityTypeMap[K][] = []
    const iterationMax = 30
    let iterationNum = 0

    while (iterationNum < iterationMax) {
      iterationNum += 1
      const parentRow = parent as TEntityTypeMap[K]
      const parentId = parentRow['parent' + this.idSuffix]

      if (!parentId && parentId !== 0) return parents.reverse()

      const parentBefore = parent
      parent = this.get(from, parentId as string | number)
      if (!parent) {
        const parentBeforeRow = parentBefore
        console.error(
          'getParents() type',
          from,
          'cannot find id',
          parentBeforeRow['parent' + this.idSuffix],
        )
        return []
      }
      parents.push(parent)
    }
    console.error('getParents()', from, id, 'iterationMax reached')
    return []
  }
  addMeta(
    userData?: Record<string, unknown>,
    dbSchema?: Record<string, unknown>[],
  ): void {
    this.loader.addMeta(userData, dbSchema)
  }
  getLastModifTimestamp(): number {
    return this.loader.getLastModifTimestamp()
  }

  get use(): Partial<Record<keyof TEntityTypeMap, boolean>> {
    return this.usageCache
  }

  get useRecursive(): Partial<Record<keyof TEntityTypeMap, boolean>> {
    return this.recursiveUsageCache
  }

  private computeUsage(): void {
    this.usageCache = {}
    this.recursiveUsageCache = {}

    for (const entity in this.tables) {
      const table = this.tables[entity]
      if (!Array.isArray(table)) continue
      if (table.length === 0) continue
      if (entity.includes('_')) continue
      ;(this.usageCache as Record<string, boolean>)[entity] = true

      const firstItem = table[0]
      if (
        firstItem &&
        typeof firstItem === 'object' &&
        'parent' + this.idSuffix in firstItem
      ) {
        ;(this.recursiveUsageCache as Record<string, boolean>)[entity] = true
      }
    }
  }

  getSchema(): Schema {
    return structuredClone(this.metadata.schema)
  }

  async checkIntegrity(): Promise<IntegrityResult> {
    await this.loader.loadTables(this.config.path, false)
    return this.integrityChecker.check(
      this.loader.db,
      this.loader.metadata.tables,
    )
  }
}
