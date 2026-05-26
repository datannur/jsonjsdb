import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import Jsonjsdb from '../src/Jsonjsdb'

type JsonjsdbPrivate = {
  computeUsage: () => void
}

type LoaderPrivate = {
  validIdChars: string
  validIdPattern: RegExp
  invalidIdPattern: RegExp
  standardizeId: (id: string) => string
}

type MutableJsonjsdb = Jsonjsdb & {
  insert: (
    table: string,
    row: Record<string, unknown>,
  ) => Record<string, unknown>
  update: (
    table: string,
    id: string | number,
    patch: Record<string, unknown>,
  ) => Record<string, unknown> | undefined
  addRelation: (
    table: string,
    id: string | number,
    relationField: string,
    relatedId: string | number,
    options?: { ifExists?: 'throw' | 'ignore' },
  ) => boolean
  addRelations: (
    table: string,
    id: string | number,
    relationField: string,
    relatedIds: Array<string | number>,
    options?: { ifExists?: 'throw' | 'ignore' },
  ) => { added: Array<string | number>; ignored: Array<string | number> }
  countRelated: (
    table: string,
    id: string | number,
    relatedTable: string,
    relationKey?: string,
  ) => number
}

describe('jsonjsdb', () => {
  let db: Jsonjsdb

  beforeAll(() => {
    db = new Jsonjsdb({
      dbKey: 'gdf9898fds',
      path: 'test/db',
    })
  })

  it('should exist', () => {
    expect(db).toBeDefined()
  })

  describe('init()', () => {
    it('should work', async () => {
      const dbInit = await db.init()
      expect(dbInit).not.toBe(false)
    })
  })

  describe('init({ filterBuilder })', () => {
    it('should ignore an empty filterBuilder result', async () => {
      const dbInit = await new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
      }).init({
        filterBuilder: () => null,
      })

      expect(dbInit.getAll('user')).toHaveLength(5)
    })

    it('should apply a single filter before creating indexes', async () => {
      const filterBuilder = vi.fn(() => ({
        entity: 'user',
        variable: 'name',
        values: ['user 1'],
      }))
      const dbInit = await new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
      }).init({
        filterBuilder,
      })

      expect(filterBuilder).toHaveBeenCalledOnce()
      expect(dbInit.get('user', 1)).toBeUndefined()
      expect(dbInit.getAll('user').map(user => user.id)).toEqual([2, 3, 4, 5])
      expect(dbInit.metadata.index.user.id[1]).toBeUndefined()
      expect(dbInit.getAll('email', { user: 1 })).toEqual([])
    })

    it('should apply multiple filters', async () => {
      const dbInit = await new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
      }).init({
        filterBuilder: () => [
          {
            entity: 'user',
            variable: 'name',
            values: ['user 2'],
          },
          {
            entity: 'tag',
            variable: 'name',
            values: ['tag 4'],
          },
        ],
      })

      expect(dbInit.getAll('user').map(user => user.id)).toEqual([1, 3, 4, 5])
      expect(dbInit.getAll('tag').map(tag => tag.id)).toEqual([1, 2, 3, 5])
      expect(dbInit.metadata.index.user.id[2]).toBeUndefined()
      expect(dbInit.metadata.index.tag.id[4]).toBeUndefined()
    })

    it('should build filters from already loaded tables', async () => {
      const dbInit = await new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
      }).init({
        filterBuilder: tables => {
          const config = tables.config.find(row => {
            return (
              typeof row === 'object' &&
              row !== null &&
              'id' in row &&
              row.id === 'app_name'
            )
          })

          if (!config) return undefined
          return {
            entity: 'user',
            variable: 'name',
            values: ['user 5'],
          }
        },
      })

      expect(dbInit.getAll('user').map(user => user.id)).toEqual([1, 2, 3, 4])
      expect(dbInit.metadata.index.user.id[5]).toBeUndefined()
    })
  })

  describe('load()', () => {
    it('should load records', async () => {
      const users = await db.load('', 'user')
      expect(users).toBeInstanceOf(Array)
      expect(users.length).toBeGreaterThan(0)
      expect(users[0]).toHaveProperty('id')
    })
  })

  describe('after init done', () => {
    beforeAll(async () => {
      await db.init()
    })

    describe('get()', () => {
      it('should get a user by id', () => {
        const user = db.get('user', 1)
        expect(user).toHaveProperty('id', 1)
      })

      it('should return undefined for nonexistent table', () => {
        const consoleSpy = vi
          .spyOn(console, 'error')
          .mockImplementation(() => {})
        const result = db.get('nonexistent_table', 1)
        expect(result).toBeUndefined()
        consoleSpy.mockRestore()
      })

      it('should return undefined for nonexistent id', () => {
        const consoleSpy = vi
          .spyOn(console, 'error')
          .mockImplementation(() => {})
        const result = db.get('user', 999)
        expect(result).toBeUndefined()
        consoleSpy.mockRestore()
      })
    })

    describe('getAll()', () => {
      it('should run without error', () => {
        expect(() => db.getAll('user')).not.toThrow()
      })

      it('should return empty array for nonexistent table', () => {
        const result = db.getAll('nonexistent_table')
        expect(result).toBeInstanceOf(Array)
        expect(result).toHaveLength(0)
      })

      it('should return 2 records when limit is 2', () => {
        const result = db.getAll('user', undefined, { limit: 2 })
        expect(result).toHaveLength(2)
        expect(result.map(user => user.id)).toEqual([1, 2])
      })

      it('should work if an id is passed', () => {
        const result = db.getAll('email', { user: 1 })
        expect(result).toHaveLength(1)
        expect(result.map(email => email.id)).toEqual(['email_1'])
      })

      it('should work if an object is passed', () => {
        const user = { id: 1 }
        const result = db.getAll('email', { user })
        expect(result).toHaveLength(1)
        expect(result.map(email => email.id)).toEqual(['email_1'])
      })

      it('should return empty array when foreign id has no related rows', () => {
        const result = db.getAll('email', { user: 999 })
        expect(result).toEqual([])
      })

      it('should get user 1 docs', () => {
        const user = db.get('user', 1)
        expect(user).toBeDefined()
        expect(user!.id).toBeDefined()
        const docs = db.getAll('doc', { user: user!.id })
        expect(docs).toBeInstanceOf(Array)
        expect(docs.map(doc => doc.id)).toEqual([3, 4, 5])
      })

      it('should apply limit when reading related rows through an array index', () => {
        const docs = db.getAll('doc', { user: 1 }, { limit: 2 })
        expect(docs.map(doc => doc.id)).toEqual([3, 4])
      })

      it('should get tags for a user through the many-to-many index', () => {
        const tags = db.getAll('tag', { user: 1 })
        expect(tags.map(tag => tag.id)).toEqual([1, 2])
      })

      it('should get users for a tag through the reverse many-to-many index', () => {
        const users = db.getAll('user', { tag: 2 })
        expect(users.map(user => user.id)).toEqual([1, 2])
      })

      it('should apply limit when reading related rows through a many-to-many index', () => {
        const users = db.getAll('user', { tag: 2 }, { limit: 1 })
        expect(users.map(user => user.id)).toEqual([1])
      })

      it('should return one row for a single-value many-to-many index', () => {
        const tags = db.getAll('tag', { user: 2 })
        expect(tags.map(tag => tag.id)).toEqual([2])
      })

      it('should filter by a role-qualified relation key', () => {
        const emails = db.getAll('email', { adminUser: 2 })
        expect(emails.map(email => email.id)).toEqual(['email_1'])
      })

      it('should filter by a direct role-qualified relation field', () => {
        const emails = db.getAll('email', { adminUserId: 2 })
        expect(emails.map(email => email.id)).toEqual(['email_1'])
      })

      it('should filter by a role-qualified relation object', () => {
        const user = db.get('user', 3)
        expect(user).toBeDefined()

        const adminUser = { id: user!.id as string | number }
        const emails = db.getAll('email', { adminUser })
        expect(emails.map(email => email.id)).toEqual(['email_2'])
      })

      it('should keep role-qualified relation filters independent', () => {
        const adminEmails = db.getAll('email', { adminUser: 3 })
        const partnerEmails = db.getAll('email', { partnerUser: 3 })

        expect(adminEmails.map(email => email.id)).toEqual(['email_2'])
        expect(partnerEmails.map(email => email.id)).toEqual(['email_1'])
      })

      it('should keep exact role-less relation keys working when role-qualified relations exist', () => {
        const emails = db.getAll('email', { user: 1 })
        expect(emails.map(email => email.id)).toEqual(['email_1'])
      })

      it('should read role-qualified multi-relations independently', () => {
        const users = db.getAll('user', { sourceUser: 1 })
        expect(users.map(user => user.id)).toEqual([2, 3])
      })

      it('should read role-qualified multi-relations in the reverse direction', () => {
        const users = db.getAll('user', { sourceOfUser: 3 })
        expect(users.map(user => user.id)).toEqual([1, 2])
      })

      it('should use the longest matching table name for role-qualified fields', () => {
        const dbWithOverlappingTables = new Jsonjsdb()
        const datasetIndex = Object.fromEntries([[1, 0]])
        const metaDatasetIndex = Object.fromEntries([[10, 0]])
        const variableIndex = Object.fromEntries([
          [1, 0],
          [2, 1],
        ])
        const variableMetaDatasetIndex = Object.fromEntries([
          [10, 0],
          [999, 1],
        ])

        Object.assign(dbWithOverlappingTables, {
          tables: {
            dataset: [{ id: 1, name: 'dataset 1' }],
            metaDataset: [{ id: 10, name: 'meta dataset 10' }],
            variable: [
              { id: 1, name: 'variable 1', metaDatasetId: 10 },
              { id: 2, name: 'variable 2', metaDatasetId: 999 },
            ],
          },
          metadata: {
            index: {
              dataset: { id: datasetIndex },
              metaDataset: { id: metaDatasetIndex },
              variable: {
                id: variableIndex,
                metaDatasetId: variableMetaDatasetIndex,
              },
            },
            schema: { oneToOne: [], oneToMany: [], manyToMany: [] },
            tables: [],
          },
        })

        const variables = dbWithOverlappingTables.getAll('variable', {
          metaDataset: 10,
        })
        expect(variables.map(variable => variable.id)).toEqual([1])
      })
    })

    describe('foreach()', () => {
      it('should run a callback for every row', () => {
        db.foreach('user', user => {
          expect(user).toHaveProperty('id')
        })
      })
    })

    describe('exists()', () => {
      it('should return false for nonexistent entity', () => {
        const result = db.exists('nonexistent_entity', 1)
        expect(result).toBe(false)
      })

      it('should return false for nonexistent id', () => {
        const result = db.exists('user', 999)
        expect(result).toBe(false)
      })

      it('should return true for existing record', () => {
        const result = db.exists('user', 1)
        expect(result).toBe(true)
      })

      it('should return false for table without id index', () => {
        const result = db.exists('nonexistent_table', 1)
        expect(result).toBe(false)
      })
    })

    describe('countRelated()', () => {
      it('should return 0 for nonexistent related table', () => {
        const result = db.countRelated('user', 1, 'nonexistent_table')
        expect(result).toBe(0)
      })

      it('should return 0 for nonexistent record', () => {
        const result = db.countRelated('user', 999, 'email')
        expect(result).toBe(0)
      })

      it('should count related records correctly', () => {
        const emailCount = db.countRelated('user', 1, 'email')
        expect(emailCount).toBe(1)
      })

      it('should count docs for user correctly', () => {
        const docCount = db.countRelated('user', 1, 'doc')
        expect(docCount).toBe(3)
      })

      it('should count many-to-many rows from the left side', () => {
        const tagCount = db.countRelated('user', 1, 'tag')
        expect(tagCount).toBe(2)
      })

      it('should count many-to-many rows from the right side', () => {
        const userCount = db.countRelated('tag', 2, 'user')
        expect(userCount).toBe(2)
      })

      it('should count a single many-to-many row', () => {
        const tagCount = db.countRelated('user', 2, 'tag')
        expect(tagCount).toBe(1)
      })

      it('should count role-qualified one-to-many rows', () => {
        const mutableDb = db as MutableJsonjsdb
        expect(mutableDb.countRelated('user', 2, 'email', 'adminUser')).toBe(1)
        expect(mutableDb.countRelated('user', 3, 'email', 'partnerUser')).toBe(
          1,
        )
      })

      it('should count role-qualified many-to-many rows', () => {
        const mutableDb = db as MutableJsonjsdb
        expect(mutableDb.countRelated('user', 1, 'user', 'sourceUser')).toBe(2)
        expect(mutableDb.countRelated('user', 3, 'user', 'sourceOfUser')).toBe(
          2,
        )
      })

      it('should return 0 when no related records exist', () => {
        // Use a user that likely has no related records
        const result = db.countRelated('user', 999, 'email')
        expect(result).toBe(0)
      })
    })

    describe('controlled mutations', () => {
      let mutableDb: MutableJsonjsdb

      beforeEach(async () => {
        mutableDb = new Jsonjsdb({
          dbKey: 'gdf9898fds',
          path: 'test/db',
        }) as MutableJsonjsdb
        await mutableDb.init()
        expect(typeof mutableDb.update).toBe('function')
        expect(typeof mutableDb.insert).toBe('function')
        expect(typeof mutableDb.addRelation).toBe('function')
        expect(typeof mutableDb.addRelations).toBe('function')
      })

      describe('update()', () => {
        it('should update a non-relational field in place', () => {
          const result = mutableDb.update('user', 1, { name: 'updated user 1' })

          expect(result).toHaveProperty('id', 1)
          expect(result).toHaveProperty('name', 'updated user 1')
          expect(mutableDb.get('user', 1)).toHaveProperty(
            'name',
            'updated user 1',
          )
          expect(mutableDb.getAll('user').map(user => user.id)).toEqual([
            1, 2, 3, 4, 5,
          ])
        })

        it('should leave indexes unchanged after updating a non-relational field', () => {
          mutableDb.update('user', 1, { name: 'updated user 1' })

          expect(
            mutableDb.getAll('email', { user: 1 }).map(email => email.id),
          ).toEqual(['email_1'])
          expect(
            mutableDb.getAll('tag', { user: 1 }).map(tag => tag.id),
          ).toEqual([1, 2])
          expect(mutableDb.countRelated('user', 1, 'tag')).toBe(2)
        })

        it('should return undefined when updating a missing row', () => {
          const result = mutableDb.update('user', 999, { name: 'missing' })

          expect(result).toBeUndefined()
        })

        it('should reject updates to indexed or relational fields', () => {
          expect(() => mutableDb.update('user', 1, { id: 10 })).toThrow()
          expect(() => mutableDb.update('user', 1, { parentId: 2 })).toThrow()
          expect(() =>
            mutableDb.update('email', 'email_1', { userId: 2 }),
          ).toThrow()
          expect(() =>
            mutableDb.update('user', 1, { docIds: '1, 2' }),
          ).toThrow()

          expect(mutableDb.get('user', 1)).toHaveProperty('id', 1)
          expect(
            mutableDb.getAll('email', { user: 1 }).map(email => email.id),
          ).toEqual(['email_1'])
          expect(
            mutableDb.getAll('doc', { user: 1 }).map(doc => doc.id),
          ).toEqual([3, 4, 5])
        })
      })

      describe('insert()', () => {
        it('should append a row and update the primary index', () => {
          const result = mutableDb.insert('user', {
            id: 6,
            name: 'user 6',
          })

          expect(result).toHaveProperty('id', 6)
          expect(mutableDb.get('user', 6)).toHaveProperty('name', 'user 6')
          expect(mutableDb.exists('user', 6)).toBe(true)
          expect(mutableDb.getAll('user').map(user => user.id)).toEqual([
            1, 2, 3, 4, 5, 6,
          ])
        })

        it('should reject duplicate ids without appending the row', () => {
          expect(() =>
            mutableDb.insert('user', {
              id: 1,
              name: 'duplicate user',
            }),
          ).toThrow()

          expect(mutableDb.getAll('user').map(user => user.id)).toEqual([
            1, 2, 3, 4, 5,
          ])
          expect(mutableDb.get('user', 1)).toHaveProperty('name', 'user 1')
        })

        it('should reject inserts into missing tables', () => {
          expect(() =>
            mutableDb.insert('missing_table', {
              id: 1,
              name: 'missing table row',
            }),
          ).toThrow()
        })

        it('should update foreign-key indexes for an inserted row', () => {
          mutableDb.insert('email', {
            id: 'email_3',
            name: 'email 3',
            userId: 1,
            adminUserId: 2,
            partnerUserId: 3,
          })

          expect(mutableDb.get('email', 'email_3')).toHaveProperty(
            'name',
            'email 3',
          )
          expect(
            mutableDb.getAll('email', { user: 1 }).map(email => email.id),
          ).toEqual(['email_1', 'email_3'])
          expect(mutableDb.countRelated('user', 1, 'email')).toBe(2)
        })

        it('should update many-to-many indexes for inserted rows with multi-relation fields', () => {
          mutableDb.insert('user', {
            id: 6,
            name: 'user 6',
            docIds: '1, 2',
          })

          expect(
            mutableDb.getAll('doc', { user: 6 }).map(doc => doc.id),
          ).toEqual([1, 2])
          expect(mutableDb.countRelated('user', 6, 'doc')).toBe(2)
        })
      })

      describe('addRelation()', () => {
        it('should add a multi-relation and update indexes in both directions', () => {
          const result = mutableDb.addRelation('user', 1, 'tagIds', 3)

          expect(result).toBe(true)
          expect(
            mutableDb.getAll('tag', { user: 1 }).map(tag => tag.id),
          ).toEqual([1, 2, 3])
          expect(
            mutableDb.getAll('user', { tag: 3 }).map(user => user.id),
          ).toEqual([1])
          expect(mutableDb.countRelated('user', 1, 'tag')).toBe(3)
          expect(mutableDb.countRelated('tag', 3, 'user')).toBe(1)
        })

        it('should keep the source multi-relation field consistent when it exists', () => {
          mutableDb.addRelation('user', 1, 'docIds', 2)

          expect(mutableDb.get('user', 1)).toHaveProperty(
            'docIds',
            '3, 4, 5, 2',
          )
          expect(
            mutableDb.getAll('doc', { user: 1 }).map(doc => doc.id),
          ).toEqual([3, 4, 5, 2])
        })

        it('should add a role-qualified multi-relation and update indexes in both directions', () => {
          const result = mutableDb.addRelation('user', 4, 'sourceUserIds', 1)

          expect(result).toBe(true)
          expect(
            mutableDb.getAll('user', { sourceUser: 1 }).map(user => user.id),
          ).toEqual([2, 3, 4])
          expect(
            mutableDb.getAll('user', { sourceOfUser: 4 }).map(user => user.id),
          ).toEqual([1])
          expect(mutableDb.get('user', 4)).toHaveProperty('sourceUserIds', '1')
        })

        it('should reject duplicate relations without changing indexes', () => {
          expect(() => mutableDb.addRelation('user', 1, 'tagIds', 2)).toThrow()

          expect(
            mutableDb.getAll('tag', { user: 1 }).map(tag => tag.id),
          ).toEqual([1, 2])
          expect(mutableDb.countRelated('user', 1, 'tag')).toBe(2)
        })

        it('should ignore duplicate relations when ifExists is ignore', () => {
          const result = mutableDb.addRelation('user', 1, 'tagIds', 2, {
            ifExists: 'ignore',
          })

          expect(result).toBe(false)
          expect(
            mutableDb.getAll('tag', { user: 1 }).map(tag => tag.id),
          ).toEqual([1, 2])
          expect(mutableDb.countRelated('user', 1, 'tag')).toBe(2)
        })

        it('should reject duplicate relations when stored ids and input ids use different primitive types', () => {
          expect(() => mutableDb.addRelation('user', 1, 'docIds', 3)).toThrow()

          expect(
            mutableDb.getAll('doc', { user: 1 }).map(doc => doc.id),
          ).toEqual([3, 4, 5])
          expect(mutableDb.countRelated('user', 1, 'doc')).toBe(3)
        })

        it('should reject missing source or related rows', () => {
          expect(() =>
            mutableDb.addRelation('user', 999, 'tagIds', 3),
          ).toThrow()
          expect(() =>
            mutableDb.addRelation('user', 1, 'tagIds', 999),
          ).toThrow()
        })

        it('should reject fields that are not multi-relation fields', () => {
          expect(() => mutableDb.addRelation('user', 1, 'tagId', 3)).toThrow()
          expect(() => mutableDb.addRelation('user', 1, 'name', 3)).toThrow()
        })
      })

      describe('addRelations()', () => {
        it('should add several relations and update indexes once per related id', () => {
          const result = mutableDb.addRelations('user', 1, 'tagIds', [3, 4])

          expect(result).toEqual({ added: [3, 4], ignored: [] })
          expect(
            mutableDb.getAll('tag', { user: 1 }).map(tag => tag.id),
          ).toEqual([1, 2, 3, 4])
          expect(
            mutableDb.getAll('user', { tag: 4 }).map(user => user.id),
          ).toEqual([1])
          expect(mutableDb.countRelated('user', 1, 'tag')).toBe(4)
        })

        it('should add several role-qualified relations', () => {
          const result = mutableDb.addRelations(
            'user',
            4,
            'sourceUserIds',
            [1, 2],
          )

          expect(result).toEqual({ added: [1, 2], ignored: [] })
          expect(
            mutableDb.getAll('user', { sourceUser: 2 }).map(user => user.id),
          ).toEqual([3, 4])
          expect(
            mutableDb.getAll('user', { sourceOfUser: 4 }).map(user => user.id),
          ).toEqual([1, 2])
          expect(mutableDb.get('user', 4)).toHaveProperty(
            'sourceUserIds',
            '1, 2',
          )
        })

        it('should ignore existing relations and duplicate batch ids when ifExists is ignore', () => {
          const result = mutableDb.addRelations(
            'user',
            1,
            'tagIds',
            [2, 3, 3],
            {
              ifExists: 'ignore',
            },
          )

          expect(result).toEqual({ added: [3], ignored: [2, 3] })
          expect(
            mutableDb.getAll('tag', { user: 1 }).map(tag => tag.id),
          ).toEqual([1, 2, 3])
          expect(mutableDb.countRelated('user', 1, 'tag')).toBe(3)
        })

        it('should reject duplicate relations by default without partial writes', () => {
          expect(() =>
            mutableDb.addRelations('user', 1, 'tagIds', [3, 2, 4]),
          ).toThrow()

          expect(
            mutableDb.getAll('tag', { user: 1 }).map(tag => tag.id),
          ).toEqual([1, 2])
          expect(mutableDb.countRelated('user', 1, 'tag')).toBe(2)
        })
      })
    })

    describe('getConfig()', () => {
      it('should read configuration values', () => {
        const result = db.getConfig('app_name')
        expect(result).toBe('jsonjsdb test fixture')
      })

      it('should return undefined for nonexistent id', () => {
        const result = db.getConfig('nonexistent_id')
        expect(result).toBeUndefined()
      })
    })

    describe('use and useRecursive', () => {
      it('should have use property with correct entities', () => {
        expect(db.use).toBeDefined()
        expect(typeof db.use).toBe('object')

        // These tables exist and have data
        expect(db.use.user).toBe(true)
        expect(db.use.doc).toBe(true)
        expect(db.use.email).toBe(true)
        expect(db.use.tag).toBe(true)
        expect(db.use.config).toBe(true)
        expect(db.use.connexion).toBe(true)

        // This table doesn't exist
        expect(db.use.nonexistent).toBeUndefined()
      })

      it('should have useRecursive property', () => {
        expect(db.useRecursive).toBeDefined()
        expect(typeof db.useRecursive).toBe('object')
      })

      it('should not mark tables with underscores as used', () => {
        // user_tag contains underscore, should be ignored
        expect(db.use.user_tag).toBeUndefined()
      })

      it('should detect recursive entities correctly', () => {
        const dbPrivate = db as unknown as JsonjsdbPrivate
        const originalComputeUsage = dbPrivate.computeUsage.bind(db)

        // Create mock data with proper types
        const recursiveData = [
          { id: 1, name: 'test 1', parentId: null },
          { id: 2, name: 'test 2', parentId: 1 },
        ]

        // Temporarily add the recursive table using Object.defineProperty
        Object.defineProperty(db.tables, 'recursivetable', {
          value: recursiveData,
          configurable: true,
          enumerable: true,
        })

        // Re-compute usage with mocked data
        originalComputeUsage()

        expect(db.use.recursivetable).toBe(true)
        expect(db.useRecursive.recursivetable).toBe(true)

        // Clean up - remove the test property
        delete (db.tables as Record<string, unknown>)['recursivetable']
        originalComputeUsage()
      })

      it('should not mark entities as recursive if they have no parent_id', () => {
        // Test tables don't have parent_id, so none should be recursive
        expect(db.useRecursive.user).toBeUndefined()
        expect(db.useRecursive.doc).toBeUndefined()
        expect(db.useRecursive.email).toBeUndefined()
        expect(db.useRecursive.tag).toBeUndefined()
      })

      it('should handle empty tables correctly', () => {
        const dbPrivate = db as unknown as JsonjsdbPrivate
        const originalComputeUsage = dbPrivate.computeUsage.bind(db)

        // Add an empty table temporarily
        Object.defineProperty(db.tables, 'emptytable', {
          value: [],
          configurable: true,
          enumerable: true,
        })

        originalComputeUsage()

        // Empty table should not be marked as used
        expect(db.use.emptytable).toBeUndefined()

        // Clean up
        delete (db.tables as Record<string, unknown>)['emptytable']
        originalComputeUsage()
      })
    })
  })

  describe('getSchema()', () => {
    it('should return a deep copy of the schema', async () => {
      await db.init()
      const schema = db.getSchema()

      expect(schema).toBeDefined()
      expect(schema).toHaveProperty('oneToOne')
      expect(schema).toHaveProperty('oneToMany')
      expect(schema).toHaveProperty('manyToMany')

      expect(Array.isArray(schema.oneToOne)).toBe(true)
      expect(Array.isArray(schema.oneToMany)).toBe(true)
      expect(Array.isArray(schema.manyToMany)).toBe(true)
    })

    it('should expose role-qualified one-to-many relations with their query key', async () => {
      await db.init()
      const schema = db.getSchema()

      expect(schema.oneToMany).toContainEqual(['adminUser', 'email'])
      expect(schema.oneToMany).toContainEqual(['partnerUser', 'email'])
      expect(schema.oneToMany).toContainEqual(['user', 'email'])
    })

    it('should return a deep copy that does not affect the original', async () => {
      await db.init()
      const schema1 = db.getSchema()
      const schema2 = db.getSchema()

      // Get initial lengths
      const initialOneToOneLength = schema2.oneToOne.length

      // Modify the first copy
      schema1.oneToOne.push(['test1', 'test2'])

      // The second copy should not be affected
      expect(schema2.oneToOne.length).toBe(initialOneToOneLength)
    })

    it('should return default empty schema when no schema exists', async () => {
      const dbWithoutSchema = new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
      })

      await dbWithoutSchema.init()
      const schema = dbWithoutSchema.getSchema()

      // The test database should have an empty default schema structure
      expect(schema).toHaveProperty('oneToOne')
      expect(schema).toHaveProperty('oneToMany')
      expect(schema).toHaveProperty('manyToMany')
      expect(Array.isArray(schema.oneToOne)).toBe(true)
      expect(Array.isArray(schema.oneToMany)).toBe(true)
      expect(Array.isArray(schema.manyToMany)).toBe(true)
    })
  })

  describe('checkIntegrity()', () => {
    it('should check database integrity successfully', async () => {
      const result = await db.checkIntegrity()

      expect(result).toHaveProperty('emptyId')
      expect(result).toHaveProperty('duplicateId')
      expect(result).toHaveProperty('parentIdNotFound')
      expect(result).toHaveProperty('parentIdSame')
      expect(result).toHaveProperty('foreignIdNotFound')
      expect(Array.isArray(result.emptyId)).toBe(true)
    })
  })

  describe('validIdChars configuration', () => {
    it('should pass validIdChars from Jsonjsdb to Loader via config', async () => {
      const customDb = new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
        validIdChars: 'a-z0-9',
      })

      // Check that config has been set correctly
      expect(customDb.config.validIdChars).toBe('a-z0-9')

      // Check that loader received the config
      const loaderPrivate = customDb.loader as unknown as LoaderPrivate
      expect(loaderPrivate.validIdChars).toBe('a-z0-9')

      // Verify regex patterns are created correctly
      const validIdPattern = loaderPrivate.validIdPattern
      const invalidIdPattern = loaderPrivate.invalidIdPattern

      expect(validIdPattern.test('abc123')).toBe(true)
      expect(validIdPattern.test('ABC123')).toBe(false) // Uppercase not allowed
      expect(validIdPattern.test('abc_123')).toBe(false) // Underscore not allowed

      expect('ABC_123'.replace(invalidIdPattern, '')).toBe('123')
    })

    it('should use default validIdChars when not specified', async () => {
      const defaultDb = new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
      })

      expect(defaultDb.config.validIdChars).toBe('a-zA-Z0-9_, -')

      const loaderPrivate = defaultDb.loader as unknown as LoaderPrivate
      expect(loaderPrivate.validIdChars).toBe('a-zA-Z0-9_, -')
    })

    it('should standardize IDs during data loading with custom validIdChars', async () => {
      const customDb = new Jsonjsdb({
        dbKey: 'gdf9898fds',
        path: 'test/db',
        validIdChars: 'a-zA-Z0-9_',
      })

      await customDb.init()

      // The standardizeId method should have been applied
      // Check that loader's standardizeId respects the custom config
      const loaderPrivate = customDb.loader as unknown as LoaderPrivate
      const standardizeId = loaderPrivate.standardizeId.bind(customDb.loader)

      expect(standardizeId('user-123')).toBe('user123') // Hyphen removed
      expect(standardizeId('user,123')).toBe('user123') // Comma removed
      expect(standardizeId('user_123')).toBe('user_123') // Underscore kept
    })

    it('should read validIdChars from HTML config', () => {
      // Create a mock HTML element
      const configDiv = document.createElement('div')
      configDiv.id = 'test-config'
      configDiv.setAttribute('data-path', 'test/db')
      configDiv.setAttribute('data-db-key', 'gdf9898fds')
      configDiv.setAttribute('data-valid-id-chars', '0-9a-z')
      document.body.appendChild(configDiv)

      const htmlDb = new Jsonjsdb('#test-config')

      expect(htmlDb.config.validIdChars).toBe('0-9a-z')

      const loaderPrivate = htmlDb.loader as unknown as LoaderPrivate
      expect(loaderPrivate.validIdChars).toBe('0-9a-z')

      // Cleanup
      document.body.removeChild(configDiv)
    })

    it('should handle HTML config with camelCase dataset attribute', () => {
      // Create a mock HTML element
      const configDiv = document.createElement('div')
      configDiv.id = 'test-config-camel'
      configDiv.setAttribute('data-path', 'test/db')
      configDiv.setAttribute('data-db-key', 'gdf9898fds')
      // Browser automatically converts data-valid-id-chars to dataset.validIdChars
      configDiv.dataset.validIdChars = 'A-Z'
      document.body.appendChild(configDiv)

      const htmlDb = new Jsonjsdb('#test-config-camel')

      expect(htmlDb.config.validIdChars).toBe('A-Z')

      // Cleanup
      document.body.removeChild(configDiv)
    })
  })
})
