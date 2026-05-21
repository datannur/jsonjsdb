import { describe, it, expect, beforeEach } from 'vitest'
import IntegrityChecker from '../src/IntegrityChecker'
import type { TableRow, TableInfo } from '../src/types'

type TestDatabase = {
  __table__?: TableInfo[]
  [tableName: string]: TableRow[] | TableInfo[] | undefined
}

describe('IntegrityChecker', () => {
  let checker: IntegrityChecker

  beforeEach(() => {
    checker = new IntegrityChecker()
  })

  function checkWithTables(db: TestDatabase) {
    const tables = db.__table__ || []
    delete db.__table__
    return checker.check(db as Record<string, TableRow[]>, tables)
  }

  describe('Constructor', () => {
    it('should create instance with default state', () => {
      expect(checker).toBeInstanceOf(IntegrityChecker)
    })
  })

  describe('check() - Empty database', () => {
    it('should return empty results for empty database', () => {
      const db = {
        __table__: [],
      }

      const result = checkWithTables(db)

      expect(result).toEqual({
        emptyId: [],
        duplicateId: {},
        parentIdNotFound: {},
        parentIdSame: {},
        foreignIdNotFound: {},
      })
    })
  })

  describe('check() - Empty ID detection', () => {
    it('should detect empty string IDs', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [
          { id: 1, name: 'John' },
          { id: '', name: 'Jane' },
          { id: 3, name: 'Bob' },
        ],
      }

      const result = checkWithTables(db)

      expect(result.emptyId).toContain('user')
    })

    it('should detect null IDs', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [
          { id: 1, name: 'John' },
          { id: null, name: 'Jane' },
          { id: 3, name: 'Bob' },
        ],
      }

      const result = checkWithTables(db)

      expect(result.emptyId).toContain('user')
    })

    it('should not flag valid IDs', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [
          { id: 1, name: 'John' },
          { id: 2, name: 'Jane' },
          { id: 'uuid-123', name: 'Bob' },
        ],
      }

      const result = checkWithTables(db)

      expect(result.emptyId).not.toContain('user')
    })

    it('should ignore tables where no row has an id field at all', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [{ name: 'John' }, { name: 'Jane' }, { name: 'Bob' }],
      }

      const result = checkWithTables(db)

      expect(result.emptyId).not.toContain('user')
      expect(result.duplicateId.user).toBeUndefined()
    })
  })

  describe('check() - Duplicate ID detection', () => {
    it('should detect duplicate IDs', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [
          { id: 1, name: 'John' },
          { id: 2, name: 'Jane' },
          { id: 1, name: 'Bob' },
          { id: 3, name: 'Alice' },
          { id: 2, name: 'Charlie' },
        ],
      }

      const result = checkWithTables(db)

      expect(result.duplicateId.user).toContain(1)
      expect(result.duplicateId.user).toContain(2)
      expect(result.duplicateId.user).not.toContain(3)
    })

    it('should not flag unique IDs', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [
          { id: 1, name: 'John' },
          { id: 2, name: 'Jane' },
          { id: 3, name: 'Bob' },
        ],
      }

      const result = checkWithTables(db)

      expect(result.duplicateId.user).toBeUndefined()
    })
  })

  describe('check() - Parent ID same detection', () => {
    it('should detect when parent_id equals id', () => {
      const db = {
        __table__: [{ name: 'category' }],
        category: [
          { id: 1, name: 'Electronics', parentId: null },
          { id: 2, name: 'Phones', parentId: 1 },
          { id: 3, name: 'Self-referencing', parentId: 3 },
        ],
      }

      const result = checkWithTables(db)

      expect(result.parentIdSame.category).toContain(3)
      expect(result.parentIdSame.category).not.toContain(1)
      expect(result.parentIdSame.category).not.toContain(2)
    })

    it('should handle tables without parent_id column', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [
          { id: 1, name: 'John' },
          { id: 2, name: 'Jane' },
        ],
      }

      const result = checkWithTables(db)

      expect(result.parentIdSame.user).toBeUndefined()
    })
  })

  describe('check() - Parent ID not found detection', () => {
    it('should detect invalid parent_id references', () => {
      const db = {
        __table__: [{ name: 'category' }],
        category: [
          { id: 1, name: 'Electronics', parentId: null },
          { id: 2, name: 'Phones', parentId: 1 },
          { id: 3, name: 'Invalid', parentId: 999 },
        ],
      }

      const result = checkWithTables(db)

      expect(result.parentIdNotFound.category).toContain(999)
      expect(result.parentIdNotFound.category).not.toContain(1)
    })

    it('should ignore null and empty parent_id values', () => {
      const db = {
        __table__: [{ name: 'category' }],
        category: [
          { id: 1, name: 'Electronics', parentId: null },
          { id: 2, name: 'Phones', parentId: '' },
          { id: 3, name: 'Tablets', parentId: 1 },
        ],
      }

      const result = checkWithTables(db)

      expect(result.parentIdNotFound.category).toBeUndefined()
    })
  })

  describe('check() - Foreign ID detection', () => {
    it('should detect invalid foreign key references', () => {
      const db = {
        __table__: [{ name: 'user' }, { name: 'post' }],
        user: [
          { id: 1, name: 'John' },
          { id: 2, name: 'Jane' },
        ],
        post: [
          { id: 1, title: 'Post 1', userId: 1 },
          { id: 2, title: 'Post 2', userId: 999 },
          { id: 3, title: 'Post 3', userId: 2 },
        ],
      }

      const result = checkWithTables(db)

      expect(result.foreignIdNotFound.post).toBeDefined()
      expect(result.foreignIdNotFound.post.userId).toContain(999)
      expect(result.foreignIdNotFound.post.userId).not.toContain(1)
      expect(result.foreignIdNotFound.post.userId).not.toContain(2)
    })

    it('should ignore null and empty foreign key values', () => {
      const db = {
        __table__: [{ name: 'user' }, { name: 'post' }],
        user: [{ id: 1, name: 'John' }],
        post: [
          { id: 1, title: 'Post 1', userId: 1 },
          { id: 2, title: 'Post 2', userId: null },
          { id: 3, title: 'Post 3', userId: '' },
        ],
      }

      const result = checkWithTables(db)

      expect(result.foreignIdNotFound.post).toBeUndefined()
    })

    it('should detect invalid role-qualified foreign key references', () => {
      const db = {
        __table__: [{ name: 'user' }, { name: 'email' }],
        user: [
          { id: 1, name: 'John' },
          { id: 2, name: 'Jane' },
        ],
        email: [
          { id: 1, name: 'Email 1', adminUserId: 1 },
          { id: 2, name: 'Email 2', adminUserId: 999 },
        ],
      }

      const result = checkWithTables(db)

      expect(result.foreignIdNotFound.email).toBeDefined()
      expect(result.foreignIdNotFound.email.adminUserId).toContain(999)
      expect(result.foreignIdNotFound.email.adminUserId).not.toContain(1)
    })

    it('should use the longest matching table name for foreign key validation', () => {
      const db = {
        __table__: [
          { name: 'dataset' },
          { name: 'metaDataset' },
          { name: 'variable' },
        ],
        dataset: [{ id: 999, name: 'dataset 999' }],
        metaDataset: [{ id: 1, name: 'meta dataset 1' }],
        variable: [
          { id: 1, name: 'Variable 1', metaDatasetId: 1 },
          { id: 2, name: 'Variable 2', metaDatasetId: 999 },
        ],
      }

      const result = checkWithTables(db)

      expect(result.foreignIdNotFound.variable.metaDatasetId).toContain(999)
      expect(result.foreignIdNotFound.variable.metaDatasetId).not.toContain(1)
    })

    it('should ignore parent_id in foreign key detection', () => {
      const db = {
        __table__: [{ name: 'category' }],
        category: [
          { id: 1, name: 'Electronics', parentId: null },
          { id: 2, name: 'Phones', parentId: 1 },
        ],
      }

      const result = checkWithTables(db)

      // parent_id should not be treated as a foreign key
      expect(result.foreignIdNotFound.category).toBeUndefined()
    })
  })

  describe('check() - Complex scenarios', () => {
    it('should handle multiple tables with multiple issues', () => {
      const db = {
        __table__: [{ name: 'user' }, { name: 'post' }, { name: 'comment' }],
        user: [
          { id: 1, name: 'John' },
          { id: '', name: 'Invalid User' }, // Empty ID
          { id: 1, name: 'Duplicate John' }, // Duplicate ID
        ],
        post: [
          { id: 1, title: 'Post 1', userId: 1 },
          { id: 2, title: 'Post 2', userId: 999 }, // Invalid foreign key
        ],
        comment: [
          { id: 1, text: 'Comment 1', postId: 1, userId: 1 },
          { id: 2, text: 'Comment 2', postId: 999, userId: 1 }, // Invalid postId
          { id: 3, text: 'Comment 3', postId: 1, userId: 888 }, // Invalid userId
        ],
      }

      const result = checkWithTables(db)

      // Check empty IDs
      expect(result.emptyId).toContain('user')

      // Check duplicate IDs
      expect(result.duplicateId.user).toContain(1)

      // Check foreign key violations
      expect(result.foreignIdNotFound.post.userId).toContain(999)
      expect(result.foreignIdNotFound.comment.postId).toContain(999)
      expect(result.foreignIdNotFound.comment.userId).toContain(888)
    })

    it('should handle valid database with no issues', () => {
      const db = {
        __table__: [{ name: 'user' }, { name: 'post' }],
        user: [
          { id: 1, name: 'John' },
          { id: 2, name: 'Jane' },
        ],
        post: [
          { id: 1, title: 'Post 1', userId: 1 },
          { id: 2, title: 'Post 2', userId: 2 },
        ],
      }

      const result = checkWithTables(db)

      expect(result.emptyId).toHaveLength(0)
      expect(Object.keys(result.duplicateId)).toHaveLength(0)
      expect(Object.keys(result.parentIdNotFound)).toHaveLength(0)
      expect(Object.keys(result.parentIdSame)).toHaveLength(0)
      expect(Object.keys(result.foreignIdNotFound)).toHaveLength(0)
    })
  })

  describe('check() - Edge cases', () => {
    it('should handle empty tables', () => {
      const db = {
        __table__: [{ name: 'user' }],
        user: [],
      }

      const result = checkWithTables(db)

      expect(result.emptyId).not.toContain('user')
      expect(result.duplicateId.user).toBeUndefined()
    })

    it('should handle string and number IDs consistently', () => {
      const db = {
        __table__: [{ name: 'user' }, { name: 'post' }],
        user: [
          { id: '1', name: 'John' },
          { id: 2, name: 'Jane' },
        ],
        post: [
          { id: 1, title: 'Post 1', userId: '1' },
          { id: 2, title: 'Post 2', userId: 2 },
        ],
      }

      const result = checkWithTables(db)

      // Should not report foreign key violations for string vs number IDs
      expect(result.foreignIdNotFound.post).toBeUndefined()
    })
  })
})
