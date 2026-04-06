import path from 'path'
import { promises as fs, existsSync } from 'fs'

export type TableRow = Record<string, unknown>
export type Row = unknown[]

function camelToSnake(str: string): string {
  return str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`)
}

export function transformKeysToSnake(data: Row[]): Row[] {
  if (!data || data.length === 0) return []
  const headers = data[0] as string[]
  const transformedHeaders = headers.map(header => camelToSnake(header))
  return [transformedHeaders, ...data.slice(1)]
}

function snakeToCamel(str: string): string {
  return str.replace(/_([a-z])/g, (match, letter) => letter.toUpperCase())
}

export function snakeToCamelKeys(row: TableRow): TableRow {
  const newRow: TableRow = {}
  for (const [key, value] of Object.entries(row)) {
    newRow[snakeToCamel(key)] = value
  }
  return newRow
}

// Converts a 2D matrix (first row headers) into list of objects
export function toObjects(data: Row[]): TableRow[] {
  if (!data || data.length === 0) return []
  const headers = data[0] as string[]
  const objects: TableRow[] = []
  for (const row of data.slice(1)) {
    const obj: TableRow = {}
    for (const [index, header] of headers.entries()) {
      obj[header] = row[index]
    }
    objects.push(obj)
  }
  return objects
}

// Converts list of objects to a 2D matrix (first row headers)
export function toMatrix(objects: TableRow[]): Row[] {
  if (!objects || objects.length === 0) return []
  const headers = Object.keys(objects[0])
  const rows: Row[] = [headers]
  for (const obj of objects) {
    const row = headers.map(header => obj[header])
    rows.push(row as unknown[])
  }
  return rows
}

export async function readJsonjs(filePath: string): Promise<TableRow[]> {
  if (!existsSync(filePath)) return []
  const content = await fs.readFile(filePath, 'utf8')
  return JSON.parse(content)
}

async function writeJsonjsFile(outputFile: string, name: string, data: Row[]) {
  const content = `jsonjs.data['${name}'] = ${JSON.stringify(data)}`
  await fs.writeFile(`${outputFile}.js.temp`, content, 'utf-8')
  await fs.rename(`${outputFile}.js.temp`, `${outputFile}.js`)
}

async function writeJsonFile(outputFile: string, data: TableRow[]) {
  const content = JSON.stringify(data, null, 2)
  await fs.writeFile(`${outputFile}.temp`, content, 'utf-8')
  await fs.rename(`${outputFile}.temp`, outputFile)
}

export async function writeJsonjs(
  outputDir: string,
  name: string,
  arrayOfArray: Row[],
  options: {
    toSnakeCase?: boolean
  } = {},
) {
  const { toSnakeCase = false } = options
  const outputFile = path.join(outputDir, `${name}.json`)

  arrayOfArray = toSnakeCase ? transformKeysToSnake(arrayOfArray) : arrayOfArray

  await Promise.all([
    writeJsonjsFile(outputFile, name, arrayOfArray),
    writeJsonFile(outputFile, toObjects(arrayOfArray)),
  ])

  return outputFile
}
