import { describe, it, expect, beforeEach, vi } from 'vitest'
import Jsonjsdb from '../src/Jsonjsdb'
import Loader from '../src/Loader'
import testSchema from './fixtures/test-schema.json'

type LoaderPrivate = {
  arrayToObject: (
    data: unknown[][],
    shouldStandardizeIds?: boolean,
    shouldTransformKeys?: boolean,
  ) => Record<string, unknown>[]
  applyTransform: (
    data: unknown[],
    shouldStandardizeIds?: boolean,
    shouldTransformKeys?: boolean,
  ) => unknown[]
  standardizeId: (id: string) => string
  isVariableId: (variable: string) => boolean
}

describe('Loader', () => {
  const dbKey = 'gdf9898fds'
  const path = 'test/db/' + dbKey
  let loader: Loader
  let loaderPrivate: LoaderPrivate

  beforeEach(() => {
    const db = new Jsonjsdb({ dbKey, path: 'test/db' })
    loader = db.loader
    loaderPrivate = loader as unknown as LoaderPrivate
  })

  describe('load_jsonjs()', () => {
    it('should load records without throwing an error', async () => {
      const users = await loader.loadJsonjs(path, 'user')
      expect(users).toBeInstanceOf(Array)
      expect(users.length).toBeGreaterThan(0)
      expect(users[0]).toHaveProperty('id')
    })
  })

  describe('load()', () => {
    it('should load db tables', async () => {
      await loader.load(path)
      expect(loader.db).toBeTypeOf('object')
      expect(Object.keys(loader.db).length).toBeGreaterThan(0)
    })

    it('should load db tables with cache', async () => {
      await loader.load(path, true)
      expect(loader.db).toBeTypeOf('object')
      expect(Object.keys(loader.db).length).toBeGreaterThan(0)
    })
  })

  describe('add_meta()', () => {
    it('should add metadata without schema', async () => {
      await loader.load(path)
      loader.addMeta()
      expect(loader.db).toHaveProperty('metaFolder')
      expect(loader.db).toHaveProperty('metaDataset')
      expect(loader.db).toHaveProperty('metaVariable')
    })

    it('should add metadata with JSON Schema', async () => {
      await loader.load(path)

      loader.addMeta({}, testSchema)

      expect(loader.db).toHaveProperty('metaFolder')
      expect(loader.db).toHaveProperty('metaDataset')
      expect(loader.db).toHaveProperty('metaVariable')

      expect(loader.db.metaFolder).toEqual([
        {
          id: 'data',
          name: 'data',
          description: 'Main database with catalog metadata',
          descriptionFr:
            'Base de données principale avec les metadonnées du catalogue',
          isInMeta: true,
          isInData: true,
        },
        {
          id: 'userData',
          name: 'userData',
          description: 'Personal database stored in user browser',
          descriptionFr:
            'Base de données personnelle stockée dans le navigateur',
          isInMeta: true,
          isInData: true,
        },
      ])

      const configDataset = loader.db.metaDataset.find(
        (d: Record<string, unknown>) => d.id === 'config',
      )
      expect(configDataset).toBeDefined()
      expect(configDataset).toMatchObject({
        id: 'config',
        metaFolderId: 'data',
        name: 'config',
        description: 'General information and entity descriptions',
        descriptionFr: 'Informations générales et descriptions des entités',
        isInMeta: true,
      })

      const configIdVar = loader.db.metaVariable.find(
        (v: Record<string, unknown>) => v.id === 'config---id',
      )
      expect(configIdVar).toBeDefined()
      expect(configIdVar).toMatchObject({
        id: 'config---id',
        metaDatasetId: 'config',
        name: 'id',
        description: 'Primary key of configuration element',
        descriptionFr: "Clé primaire de l'élément de configuration",
        type: 'string',
        isInMeta: true,
      })

      const configValueVar = loader.db.metaVariable.find(
        (v: Record<string, unknown>) => v.id === 'config---value',
      )
      expect(configValueVar).toBeDefined()
      expect(configValueVar).toMatchObject({
        id: 'config---value',
        metaDatasetId: 'config',
        name: 'value',
        description: 'Configuration value',
        descriptionFr: 'Valeur de configuration',
        type: 'string',
        isInMeta: true,
      })
    })
  })

  describe('arrayToObject()', () => {
    it('should convert matrix data into objects', async () => {
      await loader.load(path)

      const result = loaderPrivate.arrayToObject([
        ['id', 'name', 'age'],
        [1, 'Alice', 30],
        [2, 'Bob'],
      ])

      expect(result).toEqual([
        { id: 1, name: 'Alice', age: 30 },
        { id: 2, name: 'Bob' },
      ])
    })

    it('should transform snake case headers by default', async () => {
      const result = loaderPrivate.arrayToObject([
        ['colonne_entree', 'colonneEntree', 'Nom colonne'],
        ['one', 'two', 'three'],
      ])

      expect(result).toEqual([{ colonneEntree: 'two', 'Nom colonne': 'three' }])
    })

    it('should preserve headers when key transform is disabled', async () => {
      const result = loaderPrivate.arrayToObject(
        [
          ['colonne_entree', 'colonneEntree', 'Nom colonne'],
          ['one', 'two', 'three'],
        ],
        true,
        false,
      )

      expect(result).toEqual([
        {
          colonne_entree: 'one',
          colonneEntree: 'two',
          'Nom colonne': 'three',
        },
      ])
    })
  })

  describe('standardizeId()', () => {
    it('should not modify valid IDs', async () => {
      expect(loaderPrivate.standardizeId('user123')).toBe('user123')
      expect(loaderPrivate.standardizeId('abc-def')).toBe('abc-def')
      expect(loaderPrivate.standardizeId('abc_def')).toBe('abc_def')
      expect(loaderPrivate.standardizeId('abc,def')).toBe('abc,def')
      expect(loaderPrivate.standardizeId('ABC123')).toBe('ABC123')
    })

    it('should remove invalid characters from IDs', async () => {
      expect(loaderPrivate.standardizeId('user@123')).toBe('user123')
      expect(loaderPrivate.standardizeId('user 123')).toBe('user 123') // spaces are valid
      expect(loaderPrivate.standardizeId('user#123')).toBe('user123')
      expect(loaderPrivate.standardizeId('user$123')).toBe('user123')
      expect(loaderPrivate.standardizeId('user!@#$123')).toBe('user123')
    })

    it('should trim leading and trailing spaces', async () => {
      expect(loaderPrivate.standardizeId(' user123 ')).toBe('user123')
      expect(loaderPrivate.standardizeId('user\t123')).toBe('user123') // tabs are invalid
      expect(loaderPrivate.standardizeId('user\n123')).toBe('user123') // newlines are invalid
      expect(loaderPrivate.standardizeId('A B C')).toBe('A B C') // internal spaces are kept
      expect(loaderPrivate.standardizeId(' tag1, tag2 ')).toBe('tag1, tag2') // trim but keep internal spaces
    })

    it('should handle custom validIdChars configuration', async () => {
      const db = new Jsonjsdb({
        dbKey,
        path: 'test/db',
        validIdChars: 'a-z0-9',
      })
      const customLoader = db.loader
      const customLoaderPrivate = customLoader as unknown as LoaderPrivate

      expect(customLoaderPrivate.standardizeId('abc123')).toBe('abc123')
      expect(customLoaderPrivate.standardizeId('ABC123')).toBe('123')
      expect(customLoaderPrivate.standardizeId('user_id')).toBe('userid')
      expect(customLoaderPrivate.standardizeId('user-id')).toBe('userid')
    })
  })

  describe('arrayToObject() with ID standardization', () => {
    it('should standardize IDs in columns ending with _id', async () => {
      const result = loaderPrivate.arrayToObject(
        [
          ['id', 'user_id', 'name'],
          ['usr@001', 'admin#123', 'Alice'],
          ['usr 002', 'user 456', 'Bob'],
        ],
        true,
      )

      expect(result).toEqual([
        { id: 'usr001', userId: 'admin123', name: 'Alice' },
        { id: 'usr 002', userId: 'user 456', name: 'Bob' }, // internal spaces kept
      ])
    })

    it('should standardize IDs in columns ending with _ids', async () => {
      const result = loaderPrivate.arrayToObject(
        [
          ['id', 'tag_ids', 'name'],
          ['1', 'tag@1,tag 2', 'Item 1'],
          ['2', 'tag#3', 'Item 2'],
        ],
        true,
      )

      expect(result).toEqual([
        { id: '1', tagIds: 'tag1,tag 2', name: 'Item 1' }, // space after comma kept
        { id: '2', tagIds: 'tag3', name: 'Item 2' },
      ])
    })

    it('should not standardize non-ID columns', async () => {
      const result = loaderPrivate.arrayToObject(
        [
          ['id', 'email', 'description'],
          ['1', 'user@example.com', 'Test #1'],
          ['2', 'admin@test.com', 'Item #2'],
        ],
        true,
      )

      expect(result).toEqual([
        { id: '1', email: 'user@example.com', description: 'Test #1' },
        { id: '2', email: 'admin@test.com', description: 'Item #2' },
      ])
    })

    it('should respect shouldStandardizeIds=false', async () => {
      const result = loaderPrivate.arrayToObject(
        [
          ['id', 'user_id', 'name'],
          ['usr@001', 'admin#123', 'Alice'],
        ],
        false,
      )

      expect(result).toEqual([
        { id: 'usr@001', userId: 'admin#123', name: 'Alice' },
      ])
    })

    it('should standardize IDs independently from key transform', async () => {
      const result = loaderPrivate.arrayToObject(
        [
          ['id', 'user_id', 'name'],
          ['usr@001', 'admin#123', 'Alice'],
        ],
        true,
        false,
      )

      expect(result).toEqual([
        { id: 'usr001', user_id: 'admin123', name: 'Alice' },
      ])
    })

    it('should preserve ID values when standardization is disabled and key transform is disabled', async () => {
      const result = loaderPrivate.arrayToObject(
        [
          ['id', 'user_id', 'name'],
          ['usr@001', 'admin#123', 'Alice'],
        ],
        false,
        false,
      )

      expect(result).toEqual([
        { id: 'usr@001', user_id: 'admin#123', name: 'Alice' },
      ])
    })
  })

  describe('applyTransform() with ID standardization', () => {
    it('should transform snake case keys by default', async () => {
      const data = [{ colonne_entree: 'one', colonneEntree: 'two' }]

      const result = loaderPrivate.applyTransform(data)

      expect(result).toEqual([{ colonneEntree: 'two' }])
    })

    it('should preserve object keys when key transform is disabled', async () => {
      const data = [
        {
          colonne_entree: 'one',
          colonneEntree: 'two',
          'Nom colonne': 'three',
        },
      ]

      const result = loaderPrivate.applyTransform(data, true, false)

      expect(result).toEqual(data)
    })

    it('should standardize IDs in object format data', async () => {
      const data = [
        { id: 'usr@001', userId: 'admin#123', name: 'Alice' },
        { id: 'usr 002', userId: 'user 456', name: 'Bob' },
      ]

      const result = loaderPrivate.applyTransform(data, true)

      expect(result).toEqual([
        { id: 'usr001', userId: 'admin123', name: 'Alice' },
        { id: 'usr 002', userId: 'user 456', name: 'Bob' }, // internal spaces kept
      ])
    })

    it('should handle parentId field', async () => {
      const data = [{ id: 'cat@001', parentId: 'cat 000', name: 'Subcategory' }]

      const result = loaderPrivate.applyTransform(data, true)

      expect(result).toEqual([
        { id: 'cat001', parentId: 'cat 000', name: 'Subcategory' }, // internal space kept
      ])
    })

    it('should not standardize when shouldStandardizeIds=false', async () => {
      const data = [{ id: 'usr@001', userId: 'admin#123', name: 'Alice' }]

      const result = loaderPrivate.applyTransform(data, false)

      expect(result).toEqual([
        { id: 'usr@001', userId: 'admin#123', name: 'Alice' },
      ])
    })

    it('should standardize IDs independently from object key transform', async () => {
      const data = [{ id: 'usr@001', user_id: 'admin#123', name: 'Alice' }]

      const result = loaderPrivate.applyTransform(data, true, false)

      expect(result).toEqual([
        { id: 'usr001', user_id: 'admin123', name: 'Alice' },
      ])
    })

    it('should handle empty arrays', async () => {
      const result = loaderPrivate.applyTransform([], true)

      expect(result).toEqual([])
    })
  })

  describe('isVariableId()', () => {
    it('should identify ID variables correctly', async () => {
      expect(loaderPrivate.isVariableId('id')).toBe(true)
      expect(loaderPrivate.isVariableId('userId')).toBe(true)
      expect(loaderPrivate.isVariableId('parentId')).toBe(true)
      expect(loaderPrivate.isVariableId('tagIds')).toBe(true)
      expect(loaderPrivate.isVariableId('categoryIds')).toBe(true)
      expect(loaderPrivate.isVariableId('name')).toBe(false)
      expect(loaderPrivate.isVariableId('email')).toBe(false)
      expect(loaderPrivate.isVariableId('idNumber')).toBe(false)
    })
  })

  describe('Cache versioning with ?v= parameter', () => {
    it('should append ?v=version to loadViaFetch URLs', async () => {
      const version = 987654321
      let fetchedUrl = ''

      const originalFetch = window.fetch
      window.fetch = vi.fn((url: string | URL | Request) => {
        fetchedUrl = url.toString()
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve([{ id: '1', name: 'Test' }]),
        } as Response)
      })

      await loader.loadViaFetch('https://example.com/db', 'testTable', {
        version,
        useCache: false,
      })

      expect(fetchedUrl).toBe(
        'https://example.com/db/testTable.json?v=' + version,
      )
      expect(fetchedUrl).toContain('?v=' + version)

      window.fetch = originalFetch
    })

    it('should use different cache entries for different versions in fetch', async () => {
      const urls: string[] = []

      const originalFetch = window.fetch
      window.fetch = vi.fn((url: string | URL | Request) => {
        urls.push(url.toString())
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve([{ id: '1', name: 'Test' }]),
        } as Response)
      })

      await loader.loadViaFetch('https://example.com/db', 'user', {
        version: 1000,
        useCache: false,
      })

      await loader.loadViaFetch('https://example.com/db', 'user', {
        version: 2000,
        useCache: false,
      })

      expect(urls[0]).toBe('https://example.com/db/user.json?v=1000')
      expect(urls[1]).toBe('https://example.com/db/user.json?v=2000')
      expect(urls[0]).not.toBe(urls[1])

      window.fetch = originalFetch
    })

    it('should append ?v= with random value when version is not provided', async () => {
      let fetchedUrl = ''

      const originalFetch = window.fetch
      window.fetch = vi.fn((url: string | URL | Request) => {
        fetchedUrl = url.toString()
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve([{ id: '1', name: 'Test' }]),
        } as Response)
      })

      await loader.loadViaFetch('https://example.com/db', 'testTable', {
        useCache: false,
      })

      expect(fetchedUrl).toContain('https://example.com/db/testTable.json?v=')
      expect(fetchedUrl).toMatch(/\?v=0\.\d+/)

      window.fetch = originalFetch
    })
  })
})
