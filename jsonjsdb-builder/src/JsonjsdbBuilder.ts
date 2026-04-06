import path from 'path'
import { promises as fs, existsSync } from 'fs'
import readExcel from 'read-excel-file/node'
import writeXlsxFile from 'write-excel-file/node'
import chokidar from 'chokidar'
import { type PluginOption } from 'vite'
import { evolutionSchema } from './evolutionSchema'
import { compareDatasets, EvolutionEntry } from './compareDatasets'
import {
  toObjects,
  toMatrix,
  readJsonjs,
  writeJsonjs,
  snakeToCamelKeys,
  transformKeysToSnake,
  TableRow,
  Row,
} from './TableSerializer'

const tableIndex = '__table__'

type MetadataObj = Record<string, number>
type Path = string
type Extension = 'xlsx'

type MetadataItem = {
  name: string
  lastModif: number
}

export class JsonjsdbBuilder {
  private inputDb: Path
  private outputDb: Path
  private extension: Extension
  private tableIndexFilename: string = `${tableIndex}.json`
  private tableIndexFile: Path
  private updateDbTimestamp: number
  private newEvoEntries: EvolutionEntry[]
  private configPath?: string

  constructor(option: { configPath?: string } = {}) {
    this.inputDb = ''
    this.outputDb = ''
    this.tableIndexFile = ''
    this.extension = 'xlsx'
    this.updateDbTimestamp = 0
    this.newEvoEntries = []
    this.configPath = option.configPath
  }

  public async updateDb(inputDb: Path): Promise<void> {
    this.setInputDb(inputDb)
    if (!existsSync(this.inputDb)) {
      console.error(`Jsonjsdb: input db folder doesn't exist: ${this.inputDb}`)
      return
    }

    this.updateDbTimestamp = Math.round(Date.now() / 1000)
    const [inputMetadata, outputMetadata] = await Promise.all([
      this.getInputMetadata(this.inputDb),
      this.getOutputMetadata(),
    ])
    await this.deleteOldFiles(inputMetadata)
    await this.updateTables(inputMetadata, outputMetadata)
    await this.saveEvolution(inputMetadata, outputMetadata)
    await this.saveMetadata(inputMetadata, outputMetadata)
  }

  public watchDb(inputDb: Path): void {
    this.setInputDb(inputDb)
    chokidar
      .watch(this.inputDb, {
        ignored: /(^|[/\\])~$/,
        persistent: true,
        ignoreInitial: true,
      })
      .on('all', (event, path) => {
        if (path.includes('evolution.xlsx')) return false
        this.updateDb(inputDb)
      })

    console.log('Jsonjsdb watching changes in', this.inputDb)
  }

  public async setOutputDb(outputDb: Path): Promise<void> {
    this.outputDb = await this.ensureOutputDb(path.resolve(outputDb))
    this.tableIndexFile = path.join(this.outputDb, this.tableIndexFilename)
  }

  public getOutputDb(): Path {
    return this.outputDb
  }

  public getTableIndexFile(): Path {
    return this.tableIndexFile
  }

  public getVitePlugin(configPath?: string): PluginOption {
    const finalConfigPath = configPath ?? this.configPath
    if (!finalConfigPath) {
      throw new Error(
        'Config path must be provided either in constructor or getVitePlugin()',
      )
    }
    return {
      name: 'jsonjsdbAddConfig',
      transformIndexHtml: {
        order: 'post',
        handler: async (html: string) => {
          return html + '\n' + (await fs.readFile(finalConfigPath, 'utf8'))
        },
      },
    }
  }

  public getVitePlugins(
    fullReload: (path: string | string[]) => PluginOption,
    configPath?: string,
  ): PluginOption[] {
    return [this.getVitePlugin(configPath), fullReload(this.tableIndexFile)]
  }

  public async updatePreview(
    subfolder: string,
    sourcePreview: Path,
  ): Promise<void> {
    const sourcePath = path.resolve(sourcePreview)
    const outputPath = path.join(this.outputDb, subfolder)
    if (!existsSync(outputPath)) await fs.mkdir(outputPath)
    const files = await fs.readdir(sourcePath)
    for (const fileName of files) {
      if (!fileName.endsWith(`.${this.extension}`)) continue
      if (fileName.startsWith('~$')) continue
      const filePath = path.join(sourcePath, fileName)
      const tableData = await readExcel(filePath)
      const name = fileName.split('.')[0]
      await writeJsonjs(outputPath, name, tableData)
    }
  }

  public async updateMdDir(mdDir: string, sourceDir: Path) {
    if (!existsSync(sourceDir)) return
    const files = await fs.readdir(sourceDir)
    for (const file of files) {
      if (!file.endsWith('.md')) continue
      const fileContent = await fs.readFile(path.join(sourceDir, file), 'utf8')
      const outFileName = file.split('.md')[0]
      const outDir = path.join(this.outputDb, mdDir)
      if (!existsSync(outDir)) await fs.mkdir(outDir, { recursive: true })
      const tableData = [['content'], [fileContent]]
      await writeJsonjs(outDir, outFileName, tableData)
    }
  }

  private setInputDb(inputDb: Path): void {
    this.inputDb = path.resolve(inputDb)
  }

  private async getInputMetadata(folderPath: Path): Promise<MetadataItem[]> {
    try {
      const files = await fs.readdir(folderPath)
      const fileModifTimes = []
      for (const fileName of files) {
        if (!fileName.endsWith(`.${this.extension}`)) continue
        if (fileName.startsWith('~$')) continue
        const filePath = path.join(folderPath, fileName)
        const stats = await fs.stat(filePath)
        const name = fileName.split('.')[0]
        fileModifTimes.push({
          name,
          lastModif: Math.round(stats.mtimeMs / 1000),
        })
      }
      return fileModifTimes
    } catch (error) {
      console.error('Jsonjsdb: get_files_lastModif error:', error)
      return []
    }
  }

  private async getOutputMetadata(): Promise<MetadataItem[]> {
    if (!existsSync(this.tableIndexFile)) return []
    const fileContent = await fs.readFile(this.tableIndexFile, 'utf-8')
    const metadata = JSON.parse(fileContent)
    return metadata.map((row: TableRow) =>
      snakeToCamelKeys(row),
    ) as MetadataItem[]
  }

  private metadataListToObject(list: MetadataItem[]): MetadataObj {
    return list.reduce((acc: MetadataObj, row) => {
      acc[row.name] = row.lastModif
      return acc
    }, {})
  }

  private async ensureOutputDb(outputDb: Path): Promise<Path> {
    if (!existsSync(outputDb)) {
      await fs.mkdir(outputDb)
      return outputDb
    }
    const items = await fs.readdir(outputDb, { withFileTypes: true })
    const files = items.filter(
      item => item.isFile() && item.name.endsWith('.json'),
    ).length
    if (files > 0) return outputDb
    const folders = items.filter(item => item.isDirectory())
    if (folders.length !== 1) return outputDb
    return path.join(outputDb, folders[0].name)
  }

  private async deleteOldFiles(
    inputMetadata: MetadataItem[],
  ): Promise<boolean> {
    const deletePromises = []
    const inputMetadataObj = this.metadataListToObject(inputMetadata)
    const outputFiles = await fs.readdir(this.outputDb)
    for (const fileName of outputFiles) {
      const table = fileName.split('.')[0]
      if (!fileName.endsWith(`.json.js`) && !fileName.endsWith(`.json`))
        continue
      if (
        fileName === `${tableIndex}.json.js` ||
        fileName === `${tableIndex}.json`
      )
        continue
      if (table in inputMetadataObj) continue
      if (table === 'evolution') continue
      const filePath = path.join(this.outputDb, fileName)
      console.log(`Jsonjsdb: deleting ${table}`)
      deletePromises.push(fs.unlink(filePath))
    }
    await Promise.all(deletePromises)
    return deletePromises.length > 0
  }

  private async saveMetadata(
    inputMetadata: MetadataItem[],
    outputMetadata: MetadataItem[],
  ): Promise<void> {
    outputMetadata = outputMetadata.filter(row => row.name !== tableIndex)
    if (JSON.stringify(inputMetadata) === JSON.stringify(outputMetadata)) return
    inputMetadata.push({
      name: tableIndex,
      lastModif: Math.round(Date.now() / 1000),
    })
    const metadataMatrix = toMatrix(inputMetadata)
    await writeJsonjs(this.outputDb, tableIndex, metadataMatrix, {
      toSnakeCase: true,
    })
  }

  private async updateTables(
    inputMetadata: MetadataItem[],
    outputMetadata: MetadataItem[],
  ): Promise<boolean> {
    const outputMetadataObj = this.metadataListToObject(outputMetadata)
    this.newEvoEntries = []
    const updatePromises = []
    for (const row of inputMetadata) {
      const isInOutput = row.name in outputMetadataObj
      if (isInOutput && outputMetadataObj[row.name] >= row.lastModif) continue
      if (row.name === 'evolution') continue
      updatePromises.push(
        this.updateTable(row.name).then(changed => {
          if (!changed && isInOutput) {
            row.lastModif = outputMetadataObj[row.name]
          }
        }),
      )
    }
    await Promise.all(updatePromises)
    return updatePromises.length > 0
  }

  private async saveEvolution(
    inputMetadata: MetadataItem[],
    outputMetadata: MetadataItem[],
  ): Promise<void> {
    const evolutionFileJsonjs = path.join(this.outputDb, `evolution.json`)
    const evolutionFile = path.join(this.inputDb, `evolution.xlsx`)
    if (this.newEvoEntries.length > 0) {
      let evolution: TableRow[] = []
      if (existsSync(evolutionFile)) {
        const evolutionRaw = await readExcel(evolutionFile)
        const evolutionObjects = toObjects(evolutionRaw as Row[])
        evolution = evolutionObjects.map(row => snakeToCamelKeys(row))
      }
      evolution.push(
        ...this.newEvoEntries.map(entry => entry as unknown as TableRow),
      )
      const evolutionList = toMatrix(evolution as TableRow[])
      await writeJsonjs(this.outputDb, 'evolution', evolutionList, {
        toSnakeCase: true,
      })
      await writeXlsxFile(evolution, {
        schema: evolutionSchema,
        filePath: evolutionFile,
      })
    }

    if (existsSync(evolutionFileJsonjs)) {
      let evoFound = false
      for (const inputMetadataRow of inputMetadata) {
        if (inputMetadataRow.name === 'evolution') {
          evoFound = true
          if (this.newEvoEntries.length > 0) {
            const stats = await fs.stat(evolutionFile)
            inputMetadataRow.lastModif = Math.round(stats.mtimeMs / 1000)
          }
        }
      }
      if (!evoFound) {
        const outputEvo = outputMetadata.find(row => row.name === 'evolution')
        inputMetadata.push({
          name: 'evolution',
          lastModif: outputEvo?.lastModif ?? Math.round(Date.now() / 1000),
        })
      }
    }
  }

  private async updateTable(table: string): Promise<boolean> {
    const inputFile = path.join(this.inputDb, `${table}.xlsx`)
    const outputJsonFile = path.join(this.outputDb, `${table}.json`)
    const tableData = await readExcel(inputFile)
    const newData = toObjects(transformKeysToSnake(tableData as Row[]))
    const oldData = await readJsonjs(outputJsonFile)
    if (JSON.stringify(newData) === JSON.stringify(oldData)) return false
    await this.addNewEvoEntries(table, tableData, oldData)
    await writeJsonjs(this.outputDb, table, tableData, { toSnakeCase: true })
    console.log(`Jsonjsdb updating ${table}`)
    return true
  }

  private async addNewEvoEntries(
    table: string,
    tableData: Row[],
    oldTableData: TableRow[],
  ): Promise<void> {
    const newEvoEntries = compareDatasets(
      oldTableData,
      toObjects(tableData),
      this.updateDbTimestamp,
      table,
    )
    this.newEvoEntries.push(...newEvoEntries)
  }
}
