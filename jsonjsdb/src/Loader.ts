import DBrowser from './DBrowser'
import type {
  PartialJsonjsdbConfig,
  DatabaseMetadata,
  TableInfo,
  TableRow,
} from './types'

type LoadOption = {
  filter?: {
    entity?: string
    variable?: string
    values?: string[]
  }
  aliases?: Array<{ table: string; alias: string }>
  useCache?: boolean
  version?: number | string
  shouldStandardizeIds?: boolean
}

declare global {
  interface Window {
    jsonjs: {
      data?: Record<string, unknown[]>
    }
  }
}

function toSnakeCase(str: string): string {
  return str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`)
}
function snakeToCamel(str: string): string {
  return str.replace(/_./g, match => match[1].toUpperCase())
}

export default class Loader {
  private tableIndex = '__table__'
  private cachePrefix = 'dbCache/'
  private idSuffix = 'Id'

  private browser: DBrowser
  private tableIndexCache?: Record<string, string | number | undefined>
  private lastModifTimestamp?: number
  private metaVariable: Record<string, unknown> = {}
  private validIdChars: string
  private validIdPattern: RegExp
  private invalidIdPattern: RegExp

  public db: Record<string, TableRow[]> = {}

  public metadata: DatabaseMetadata = {
    schema: {
      aliases: [],
      oneToOne: [],
      oneToMany: [],
      manyToMany: [],
    },
    index: {},
    tables: [],
    dbSchema: undefined,
  }

  constructor(browser: DBrowser, config: PartialJsonjsdbConfig) {
    window.jsonjs = {}
    this.browser = browser
    this.validIdChars = config.validIdChars ?? 'a-zA-Z0-9_, -'
    this.validIdPattern = new RegExp(`^[${this.validIdChars}]+$`)
    this.invalidIdPattern = new RegExp(`[^${this.validIdChars}]`, 'g')
  }

  async load(
    path: string,
    useCache = false,
    option: LoadOption = {},
  ): Promise<Record<string, unknown>> {
    await this.loadTables(path, useCache)
    this.normalizeSchema()
    if (option.filter?.values?.length && option.filter.values.length > 0) {
      this.filter(option.filter)
    }
    this.createAlias(option.aliases)
    this.createIndex()
    return this.db
  }

  async loadFromCache(tableName: string): Promise<unknown[]> {
    return this.browser.get(this.cachePrefix + tableName) as Promise<unknown[]>
  }

  async saveToCache(
    data: unknown[] | Record<string, unknown>,
    tableName: string,
  ): Promise<void> {
    this.browser.set(this.cachePrefix + tableName, data)
  }

  async loadViaScript(
    path: string,
    tableName: string,
    option?: LoadOption,
  ): Promise<unknown[]> {
    const script = document.createElement('script')
    let src = path + '/' + tableName + '.json.js?v='
    src += option && option.version ? option.version : Math.random()
    script.src = src
    script.async = false
    return new Promise((resolve, reject) => {
      script.onload = () => {
        if (
          !(tableName in window.jsonjs.data!) ||
          window.jsonjs.data![tableName] === undefined
        ) {
          const errorMsg = `jsonjs.data.${tableName} not found in table ${tableName}`
          console.error(errorMsg)
          reject(new Error(errorMsg))
          return
        }
        let data = window.jsonjs.data![tableName]
        delete window.jsonjs.data![tableName]
        if (data.length > 0 && Array.isArray(data[0])) {
          data = this.arrayToObject(
            data as unknown[][],
            option?.shouldStandardizeIds,
          )
        } else {
          data = this.applyTransform(data, option?.shouldStandardizeIds)
        }
        resolve(data)
        document.querySelectorAll(`script[src="${src}"]`)[0].remove()
        if (option && option.useCache) {
          this.saveToCache(data, tableName)
        }
      }
      script.onerror = () => {
        const errorMsg = `table "${tableName}" not found in path "${path}"`
        delete window.jsonjs.data![tableName]
        console.error(errorMsg)
        reject(new Error(errorMsg))
        return
      }
      document.head.appendChild(script)
    })
  }

  async loadViaFetch(
    path: string,
    tableName: string,
    option?: LoadOption,
  ): Promise<unknown[]> {
    const version = option?.version ?? Math.random()
    const url = `${path}/${tableName}.json?v=${version}`

    try {
      const response = await fetch(url, { cache: 'default' })

      if (!response.ok) {
        throw new Error(
          `HTTP error! status: ${response.status} for table "${tableName}"`,
        )
      }

      let data = (await response.json()) as unknown[]

      if (data.length > 0 && Array.isArray(data[0])) {
        data = this.arrayToObject(
          data as unknown[][],
          option?.shouldStandardizeIds,
        )
      } else {
        data = this.applyTransform(data, option?.shouldStandardizeIds)
      }

      if (option?.useCache) this.saveToCache(data, tableName)

      return data
    } catch (error) {
      const errorMsg = `Failed to load table "${tableName}" from "${url}": ${error}`
      console.error(errorMsg)
      throw new Error(errorMsg, { cause: error })
    }
  }

  private isVariableId(variable: string): boolean {
    return (
      variable === 'id' ||
      variable.endsWith(this.idSuffix) ||
      variable.endsWith(this.idSuffix + 's')
    )
  }

  private standardizeId(id: string): string {
    const trimmed = id.trim()
    if (this.validIdPattern.test(trimmed)) return trimmed
    const cleaned = trimmed.replace(this.invalidIdPattern, '')
    if (cleaned !== trimmed) {
      console.warn(`ID standardized: "${id}" → "${cleaned}"`)
    }
    return cleaned
  }

  private snakeToCamel(str: string): string {
    return str.replace(/_([a-z])/g, (match, letter) => letter.toUpperCase())
  }

  private arrayToObject(
    data: unknown[][],
    shouldStandardizeIds = true,
  ): Record<string, unknown>[] {
    if (data.length === 0) return []
    const headers = data[0].map(h => this.snakeToCamel(String(h)))
    const headersLength = headers.length
    const dataLength = data.length
    const records = new Array<Record<string, unknown>>(dataLength - 1)
    for (let i = 1; i < dataLength; i += 1) {
      const row = data[i]
      const rowObject: Record<string, unknown> = {}
      for (let j = 0; j < headersLength; j += 1) {
        const value = row[j]
        rowObject[headers[j]] =
          typeof value === 'string' &&
          shouldStandardizeIds &&
          this.isVariableId(headers[j])
            ? this.standardizeId(value)
            : value
      }
      records[i - 1] = rowObject
    }
    return records
  }

  private applyTransform(
    data: unknown[],
    shouldStandardizeIds = true,
  ): unknown[] {
    if (data.length === 0) return data

    const firstRow = data[0] as Record<string, unknown>
    const keys = Object.keys(firstRow)
    const keysLength = keys.length
    const camelKeys = keys.map(key => this.snakeToCamel(key))
    const isIdKeys = camelKeys.map(
      camelKey => shouldStandardizeIds && this.isVariableId(camelKey),
    )
    const dataLength = data.length
    const records = new Array<Record<string, unknown>>(dataLength)

    for (let i = 0; i < dataLength; i += 1) {
      const row = data[i] as Record<string, unknown>
      const rowObject: Record<string, unknown> = {}
      for (let j = 0; j < keysLength; j += 1) {
        const value = row[keys[j]]
        rowObject[camelKeys[j]] =
          typeof value === 'string' && isIdKeys[j]
            ? this.standardizeId(value)
            : value
      }
      records[i] = rowObject
    }
    return records
  }

  private isInCache(tableName: string, version?: number | string): boolean {
    if (!this.tableIndexCache) return false
    if (!(tableName in this.tableIndexCache)) return false
    if (this.tableIndexCache[tableName] !== version) return false
    return true
  }

  async loadJsonjs(
    path: string,
    tableName: string,
    option?: {
      useCache: boolean
      version: number | string
      shouldStandardizeIds?: boolean
    },
  ): Promise<unknown[]> {
    if (path.slice(-1) === '/') path = path.slice(0, -1)

    const isHttpProtocol = window.location.protocol.startsWith('http')

    if (!isHttpProtocol) {
      if (window.jsonjs === undefined) window.jsonjs = {}
      if (window.jsonjs.data === undefined) window.jsonjs.data = {}
    }

    if (option?.useCache && this.isInCache(tableName, option.version)) {
      return this.loadFromCache(tableName)
    }

    if (isHttpProtocol) {
      return this.loadViaFetch(path, tableName, option)
    } else {
      return this.loadViaScript(path, tableName, option)
    }
  }

  async loadTables(path: string, useCache: boolean): Promise<void> {
    let tablesInfo = (await this.loadJsonjs(
      path,
      this.tableIndex,
    )) as TableInfo[]
    tablesInfo = this.checkConformity(tablesInfo)
    tablesInfo = this.extractLastModif(tablesInfo)
    if (useCache) {
      this.tableIndexCache = (await this.browser.get(
        this.cachePrefix + this.tableIndex,
      )) as Record<string, string | number | undefined>
      const newTableIndexCache = tablesInfo.reduce(
        (acc, item) => {
          return { ...acc, [item.name]: item.lastModif }
        },
        {} as Record<string, string | number | undefined>,
      )
      this.saveToCache(newTableIndexCache, this.tableIndex)
    }

    this.metadata.schema = {
      aliases: [],
      oneToOne: [],
      oneToMany: [],
      manyToMany: [],
    }

    this.metadata.index = {}
    this.metadata.tables = tablesInfo

    this.db = {}
    const promises = []
    const tables = []
    for (const table of tablesInfo) {
      tables.push({ name: table.name })
      promises.push(
        this.loadJsonjs(path, table.name, {
          version: table.lastModif ?? Date.now(),
          useCache: useCache,
          shouldStandardizeIds: true,
        }),
      )
    }
    const tablesData = await Promise.all(promises)
    for (const [i, tableData] of tablesData.entries()) {
      this.db[tables[i].name] = tableData as TableRow[]
    }
  }

  getLastModifTimestamp(): number {
    return this.lastModifTimestamp || 0
  }

  extractLastModif(tablesInfo: TableInfo[]): TableInfo[] {
    const tableIndexRow = tablesInfo.filter(
      item => item.name === this.tableIndex,
    )
    if (tableIndexRow.length > 0 && tableIndexRow[0].lastModif) {
      this.lastModifTimestamp = tableIndexRow[0].lastModif as number
    }
    return tablesInfo.filter(item => item.name !== this.tableIndex)
  }

  checkConformity(tablesInfo: TableInfo[]): TableInfo[] {
    const validatedTables: TableInfo[] = []
    const allNames: string[] = []
    for (const table of tablesInfo) {
      if (!('name' in table)) {
        console.error('table name not found in meta', table)
        continue
      }
      if (allNames.includes(table.name)) {
        console.error('table name already exists in meta', table)
        continue
      }
      allNames.push(table.name)
      validatedTables.push(table)
    }
    return validatedTables
  }

  normalizeSchema() {
    for (const table of this.metadata.tables) {
      if (this.db[table.name].length === 0) continue
      for (const variable in this.db[table.name][0]) {
        if (!variable.endsWith(this.idSuffix + 's')) continue
        const entityDest = variable.slice(0, -(this.idSuffix.length + 1))
        if (!(entityDest in this.db)) continue
        const relationTable = table.name + '_' + entityDest
        if (!(relationTable in this.db)) {
          this.metadata.tables.push({ name: relationTable })
          this.db[relationTable] = []
        }
        for (const row of this.db[table.name]) {
          if (!row[variable]) continue
          const ids =
            typeof row[variable] === 'string' ? row[variable].split(',') : []
          if (ids.length === 0) continue
          for (const id of ids) {
            this.db[relationTable].push({
              [table.name + this.idSuffix]: row.id,
              [entityDest + this.idSuffix]: id.trim(),
            })
          }
        }
      }
    }
  }

  filter(filter: { entity?: string; variable?: string; values?: unknown[] }) {
    if (!('entity' in filter) || !filter.entity) return false
    if (!('variable' in filter) || !filter.variable) return false
    if (!('values' in filter) || !filter.values) return false
    if (!(filter.entity in this.db)) return false

    const idToDelete: string[] = []
    for (const item of this.db[filter.entity]) {
      if (filter.values.includes(item[filter.variable])) {
        if (item.id) {
          idToDelete.push(String(item.id))
        }
      }
    }
    this.db[filter.entity] = this.db[filter.entity].filter(
      (item: Record<string, unknown>) =>
        !idToDelete.includes(item.id as string),
    )
    for (const table of this.metadata.tables) {
      if (this.db[table.name].length === 0) continue
      if (!(filter.entity + this.idSuffix in this.db[table.name][0])) continue
      const idToDeleteLevel2: string[] = []
      for (const item of this.db[table.name]) {
        const foreignId = item[filter.entity + this.idSuffix]
        if (foreignId && idToDelete.includes(String(foreignId))) {
          if (item.id) {
            idToDeleteLevel2.push(String(item.id))
          }
        }
      }
      this.db[table.name] = this.db[table.name].filter(
        (item: Record<string, unknown>) =>
          !idToDelete.includes(item[filter.entity + this.idSuffix] as string),
      )
      for (const tableLevel2 of this.metadata.tables) {
        if (this.db[tableLevel2.name].length === 0) continue
        if (!(table.name + this.idSuffix in this.db[tableLevel2.name][0]))
          continue
        this.db[tableLevel2.name] = this.db[tableLevel2.name].filter(
          (item: Record<string, unknown>) =>
            !idToDeleteLevel2.includes(
              item[table.name + this.idSuffix] as string,
            ),
        )
      }
    }
  }

  idToIndex(tableName: string, id: number | string): number | false {
    if (!this.metadata.index[tableName]) {
      console.error('idToIndex() table not found: ', tableName)
      return false
    }
    if (this.metadata.index[tableName].id[id] === undefined) {
      console.error('idToIndex() table ', tableName, 'id not found', id)
      return false
    }
    const indexValue = this.metadata.index[tableName].id[id]
    // For id index, we expect only a single number, not an array
    return typeof indexValue === 'number' ? indexValue : false
  }

  createAlias(
    initialAliases: Array<{ table: string; alias: string }> | null = null,
  ) {
    type AliasDefinition = {
      table: string
      alias: string
    }

    let aliases: AliasDefinition[] = []

    if (initialAliases) {
      aliases = initialAliases.map(({ table, alias }) => ({ table, alias }))
    }

    if ('config' in this.db) {
      for (const row of this.db.config) {
        if (typeof row.id === 'string' && row.id.startsWith('alias_')) {
          const value = row.value
          if (typeof value === 'string') {
            const table = value.split(':')[0]?.trim()
            const alias = value.split(':')[1]?.trim()
            if (table && alias) {
              aliases.push({ table, alias })
            }
          }
        }
      }
    }

    if ('alias' in this.db) {
      aliases = aliases.concat(this.db.alias as typeof aliases)
    }

    for (const alias of aliases) {
      const aliasData: Record<string, unknown>[] = []
      if (!(alias.table in this.db)) continue
      for (const row of this.db[alias.table]) {
        const aliasDataRow: Record<string, unknown> = { id: row.id }
        aliasDataRow[alias.table + this.idSuffix] = row.id
        aliasData.push(aliasDataRow)
      }
      this.db[alias.alias] = aliasData as TableRow[]
      this.metadata.tables.push({ name: alias.alias, alias: true })
      this.metadata.schema.aliases.push(alias.alias)
    }
  }

  createIndex() {
    this.metadata.index = {}
    for (const table of this.metadata.tables) {
      if (!table.name.includes('_')) this.metadata.index[table.name] = {}
    }
    for (const table of this.metadata.tables) {
      if (!table.name.includes('_') && this.db[table.name][0]) {
        this.addPrimaryKey(table)
        this.processOneToMany(table)
      }
    }
    for (const table of this.metadata.tables) {
      if (table.name.includes('_') && this.db[table.name][0]) {
        this.processManyToMany(table, 'left')
        this.processManyToMany(table, 'right')
      }
    }
  }

  processManyToMany(table: { name: string }, side: string) {
    const index: Record<string | number, number | number[]> = {}
    const tablesName = table.name.split('_')
    if (!(tablesName[0] in this.metadata.index)) {
      console.error('processManyToMany() table not found', tablesName[0])
      return false
    }
    if (!(tablesName[1] in this.metadata.index)) {
      console.error('processManyToMany() table not found', tablesName[1])
      return false
    }
    if (side === 'right') tablesName.reverse()
    const tableNameId0 = tablesName[0] + this.idSuffix
    const tableNameId1 = tablesName[1] + this.idSuffix
    for (const row of this.db[table.name]) {
      const id0 = row[tableNameId0]
      const id1 = row[tableNameId1]

      if (id0 == null || id1 == null) continue

      const indexValue = this.idToIndex(tablesName[0], id0 as string | number)
      if (indexValue === false) continue

      const key = String(id1)
      if (!(key in index)) {
        index[key] = indexValue
        continue
      }
      if (!Array.isArray(index[key])) {
        index[key] = [index[key] as number]
      }
      ;(index[key] as number[]).push(indexValue)
    }
    delete (index as Record<string, unknown>)['null']
    this.metadata.index[tablesName[0]][tableNameId1] = index
    if (side === 'left') {
      this.metadata.schema.manyToMany.push([
        tablesName[0],
        tableNameId1.slice(0, -this.idSuffix.length),
      ])
    }
  }

  processOneToMany(table: { name: string }) {
    for (const variable in this.db[table.name][0]) {
      if (variable === 'parent' + this.idSuffix) {
        this.processSelfOneToMany(table)
        continue
      }
      if (
        variable.endsWith(this.idSuffix) &&
        variable.slice(0, -this.idSuffix.length) in this.metadata.index
      ) {
        this.addForeignKey(variable, table)
      }
    }
  }

  processSelfOneToMany(table: { name: string }) {
    const index: Record<string | number, number | number[]> = {}
    for (const [i, row] of Object.entries(this.db[table.name])) {
      const rowRecord = row as Record<string, unknown>
      const parentId = rowRecord['parent' + this.idSuffix] as string | number
      if (!(parentId in index)) {
        index[parentId] = parseInt(i)
        continue
      }
      if (!Array.isArray(index[parentId])) {
        index[parentId] = [index[parentId] as number]
      }
      ;(index[parentId] as number[]).push(parseInt(i))
    }
    this.metadata.index[table.name]['parent' + this.idSuffix] = index
    this.metadata.schema.oneToMany.push([table.name, table.name])
  }

  addPrimaryKey(table: { name: string }) {
    const index: Record<string | number, number> = {}
    if (!(table.name in this.db)) return false
    if (this.db[table.name].length === 0) return false
    if (!('id' in this.db[table.name][0])) return false
    for (const [i, row] of Object.entries(this.db[table.name])) {
      const rowRecord = row as Record<string, unknown>
      const id = rowRecord.id as string | number
      index[id] = parseInt(i)
    }
    this.metadata.index[table.name].id = index
  }

  addForeignKey(variable: string, table: { name: string }) {
    const index: Record<string, number | number[]> = {}
    for (const [i, row] of Object.entries(this.db[table.name])) {
      const rowRecord = row as Record<string, unknown>
      const foreignKeyValue = rowRecord[variable] as string
      if (!(foreignKeyValue in index)) {
        index[foreignKeyValue] = parseInt(i)
        continue
      }
      if (!Array.isArray(index[foreignKeyValue])) {
        index[foreignKeyValue] = [index[foreignKeyValue] as number]
      }
      ;(index[foreignKeyValue] as number[]).push(parseInt(i))
    }
    delete index['null']
    this.metadata.index[table.name][variable] = index
    if (this.metadata.schema.aliases.includes(table.name)) {
      this.metadata.schema.oneToOne.push([
        table.name,
        variable.slice(0, -this.idSuffix.length),
      ])
    } else {
      this.metadata.schema.oneToMany.push([
        variable.slice(0, -this.idSuffix.length),
        table.name,
      ])
    }
  }

  addDbSchema(jsonSchemas: Record<string, unknown>[]) {
    if (!Array.isArray(jsonSchemas) || jsonSchemas.length === 0) {
      this.metadata.dbSchema = []
      return
    }

    const metaRows: Record<string, unknown>[] = []

    for (const schema of jsonSchemas) {
      const schemaObj = schema as Record<string, unknown>
      const datasetName = schemaObj.title as string

      if (!datasetName || datasetName === '__table__') continue

      if (datasetName === '__meta__') {
        const dbDescriptions =
          (schemaObj['x-db-description'] as Record<string, string>) || {}
        const dbDescriptionsFr =
          (schemaObj['x-db-description-fr'] as Record<string, string>) || {}

        for (const [folder, description] of Object.entries(dbDescriptions)) {
          metaRows.push({
            folder,
            dataset: null,
            variable: null,
            description,
            descriptionFr: dbDescriptionsFr[folder] || null,
            storageKey: null,
          })
        }
        continue
      }

      const folderName = (schemaObj['x-db'] as string) || 'data'
      const datasetDescription = schemaObj.description as string
      const datasetDescriptionFr = schemaObj['x-description-fr'] as string

      metaRows.push({
        folder: folderName,
        dataset: datasetName,
        variable: null,
        description: datasetDescription,
        descriptionFr: datasetDescriptionFr || null,
        storageKey: null,
      })

      const items = schemaObj.items as Record<string, unknown>
      if (!items || !items.properties) continue

      const properties = items.properties as Record<
        string,
        Record<string, unknown>
      >
      for (const [varName, varSchema] of Object.entries(properties)) {
        metaRows.push({
          folder: folderName,
          dataset: datasetName,
          variable: snakeToCamel(varName),
          description: varSchema.description as string,
          descriptionFr: (varSchema['x-description-fr'] as string) || null,
          type: varSchema.type,
          storageKey: toSnakeCase(varName),
        })
      }
    }

    this.metadata.dbSchema = metaRows
  }

  addMeta(
    userData?: Record<string, unknown>,
    schema?: Record<string, unknown>[],
  ): void {
    const metaDataset: Record<string, Record<string, unknown>> = {}
    const metaFolder: Record<string, Record<string, unknown>> = {}
    this.metaVariable = {}

    if (schema) this.addDbSchema(schema)

    const virtualMetaTables: string[] = []
    for (const table of this.metadata.tables) {
      if (!table.lastModif) virtualMetaTables.push(table.name)
    }

    if (this.metadata.dbSchema) {
      for (const row of this.metadata.dbSchema as Record<string, unknown>[]) {
        const rowRecord = row as Record<string, unknown>
        if (!rowRecord.folder) continue
        if (!rowRecord.dataset) {
          rowRecord.id = rowRecord.folder
          metaFolder[rowRecord.id as string] = rowRecord
          continue
        }
        if (!rowRecord.variable) {
          rowRecord.id = rowRecord.dataset
          metaDataset[rowRecord.id as string] = rowRecord
          continue
        }
        rowRecord.id = rowRecord.dataset + '---' + rowRecord.variable
        this.metaVariable[rowRecord.id as string] = rowRecord
      }
    }

    const metaFolderData = {
      id: 'data',
      name: 'data',
      description: (
        (metaFolder as Record<string, unknown>).data as Record<string, unknown>
      )?.description,
      descriptionFr: (
        (metaFolder as Record<string, unknown>).data as Record<string, unknown>
      )?.descriptionFr,
      isInMeta: (metaFolder as Record<string, unknown>).data ? true : false,
      isInData: true,
    }
    const metaFolderUserData = {
      id: 'userData',
      name: 'userData',
      description: (
        (metaFolder as Record<string, unknown>).userData as Record<
          string,
          unknown
        >
      )?.description,
      descriptionFr: (
        (metaFolder as Record<string, unknown>).userData as Record<
          string,
          unknown
        >
      )?.descriptionFr,
      isInMeta: (metaFolder as Record<string, unknown>).userData ? true : false,
      isInData: true,
    }
    this.db.metaFolder = [metaFolderData, metaFolderUserData]
    this.db.metaDataset = []
    this.db.metaVariable = []

    const userDataTables = Object.entries(userData || {})
    for (const [tableName, tableData] of userDataTables) {
      const tableDataArray = tableData as unknown[]
      if (tableDataArray.length === 0) continue
      const variables = Object.keys(
        tableDataArray[0] as Record<string, unknown>,
      )
      this.db.metaDataset.push({
        id: tableName,
        ['metaFolder' + this.idSuffix]: 'userData',
        name: tableName,
        nbRow: tableDataArray.length,
        description: metaDataset[tableName]?.description,
        descriptionFr: metaDataset[tableName]?.descriptionFr,
        isInMeta: metaDataset[tableName] ? true : false,
        isInData: true,
      })
      this.addMetaVariables(tableName, tableData as unknown[], variables)
      if (tableName in metaDataset) delete metaDataset[tableName]
    }

    for (const table of this.metadata.tables) {
      if (table.name.includes(this.tableIndex)) continue
      if (this.db[table.name].length === 0) continue
      if (virtualMetaTables.includes(table.name)) continue
      const variables = Object.keys(this.db[table.name][0])
      this.db.metaDataset.push({
        id: table.name,
        ['metaFolder' + this.idSuffix]: 'data',
        name: table.name,
        nbRow: this.db[table.name].length,
        description: metaDataset[table.name]?.description,
        descriptionFr: metaDataset[table.name]?.descriptionFr,
        lastUpdateTimestamp: table.lastModif,
        isInMeta: metaDataset[table.name] ? true : false,
        isInData: true,
      })
      this.addMetaVariables(table.name, this.db[table.name], variables)
      if (table.name in metaDataset) delete metaDataset[table.name]
    }

    for (const [variableId, variable] of Object.entries(this.metaVariable)) {
      const variableRecord = variable as Record<string, unknown>
      const datasetId = variableRecord.dataset
      this.db.metaVariable.push({
        id: variableId,
        ['metaDataset' + this.idSuffix]: datasetId,
        name: variableRecord.variable,
        description: variableRecord.description,
        descriptionFr: variableRecord.descriptionFr,
        storageKey: variableRecord.storageKey,
        isInMeta: true,
        isInData: false,
      })
    }

    for (const [datasetId, dataset] of Object.entries(metaDataset)) {
      const datasetRecord = dataset as Record<string, unknown>
      this.db.metaDataset.push({
        id: datasetId,
        ['metaFolder' + this.idSuffix]: datasetRecord.folder,
        name: datasetRecord.dataset,
        nbRow: 0,
        description: datasetRecord?.description,
        descriptionFr: datasetRecord?.descriptionFr,
        isInMeta: true,
        isInData: false,
      })
    }

    this.metadata.index.metaFolder = {}
    this.metadata.index.metaDataset = {}
    this.metadata.index.metaVariable = {}
    this.addPrimaryKey({ name: 'metaFolder' })
    this.addPrimaryKey({ name: 'metaDataset' })
    this.addPrimaryKey({ name: 'metaVariable' })
    this.processOneToMany({ name: 'metaDataset' })
    this.processOneToMany({ name: 'metaVariable' })
  }

  addMetaVariables(
    tableName: string,
    datasetData: unknown[],
    variables: string[],
  ) {
    const datasetArray = datasetData as unknown[]
    const nbValueMax = Math.min(300, Math.floor(datasetArray.length / 5))
    for (const variable of variables) {
      let type = 'other'
      for (const row of datasetData) {
        const rowRecord = row as Record<string, unknown>
        const value = rowRecord[variable]
        if (value === null || value === undefined) continue
        if (typeof value === 'string') {
          type = 'string'
          break
        }
        if (typeof value === 'number' && !isNaN(value)) {
          if (Number.isInteger(value)) {
            type = 'integer'
            break
          } else {
            type = 'float'
            break
          }
        }
        if (typeof value === 'boolean') {
          type = 'boolean'
          break
        }
      }
      let nbMissing = 0
      const distincts = new Set()
      for (const row of datasetData) {
        const rowRecord = row as Record<string, unknown>
        const value = rowRecord[variable]
        if (value === '' || value === null || value === undefined) {
          nbMissing += 1
          continue
        }
        distincts.add(value)
      }
      let values: boolean | Array<{ value: unknown }> = false
      const hasValue = distincts.size < nbValueMax && distincts.size > 0
      if (hasValue) {
        values = []
        for (const value of distincts) {
          values.push({ value })
        }
      }
      const datasetVariableId = tableName + '---' + variable
      this.db.metaVariable.push({
        id: datasetVariableId,
        ['metaDataset' + this.idSuffix]: tableName,
        name: variable,
        description: (
          this.metaVariable[datasetVariableId] as Record<string, unknown>
        )?.description,
        descriptionFr: (
          this.metaVariable[datasetVariableId] as Record<string, unknown>
        )?.descriptionFr,
        storageKey: (
          this.metaVariable[datasetVariableId] as Record<string, unknown>
        )?.storageKey,
        type,
        nbMissing: nbMissing,
        nbDistinct: distincts.size,
        nbDuplicate: datasetData.length - distincts.size - nbMissing,
        values,
        valuesPreview: values ? values.slice(0, 10) : false,
        isInMeta: this.metaVariable[datasetVariableId] ? true : false,
        isInData: true,
      })
      if (datasetVariableId in this.metaVariable)
        delete this.metaVariable[datasetVariableId]
    }
  }
}
