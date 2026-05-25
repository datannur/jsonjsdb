import { describe, it, expect } from 'vitest'
import DBrowser from '../src/DBrowser'

describe('DBrowser', () => {
  const testAppName = 'test-app-dbrowser'

  describe('Constructor', () => {
    it('should create instance', () => {
      const instance = new DBrowser(testAppName)
      expect(instance).toBeInstanceOf(DBrowser)
    })
  })

  describe('Method existence', () => {
    it('should have all required methods', () => {
      const instance = new DBrowser(testAppName)
      expect(typeof instance.get).toBe('function')
      expect(typeof instance.set).toBe('function')
      expect(typeof instance.getAll).toBe('function')
      expect(typeof instance.clear).toBe('function')
    })
  })

  describe('Basic synchronous operations', () => {
    it('should call set without throwing errors', () => {
      const instance = new DBrowser(testAppName)
      expect(() => {
        instance.set('test-key', 'test-value')
      }).not.toThrow()
    })

    it('should call set with callback without throwing errors', () => {
      const instance = new DBrowser(testAppName)
      const callback = () => {}
      expect(() => {
        instance.set('test-key', 'test-value', callback)
      }).not.toThrow()
    })

    it('should call clear without throwing errors', () => {
      const instance = new DBrowser(testAppName)
      expect(() => {
        instance.clear()
      }).not.toThrow()
    })
  })

  describe('Typed storage operations', () => {
    it('should handle different data types', () => {
      const instance = new DBrowser(testAppName)

      expect(() => {
        instance.set('string', 'test string')
        instance.set('number', 123)
        instance.set('boolean', true)
        instance.set('object', { key: 'value' })
        instance.set('null', null)
        instance.set('undefined', undefined)
      }).not.toThrow()
    })
  })

  describe('getAll operations', () => {
    it('should call getAll without throwing errors', () => {
      const instance = new DBrowser(testAppName)
      const callback = () => {}
      expect(() => {
        instance.getAll('test-prefix', callback)
      }).not.toThrow()
    })
  })

  describe('Simple async operations', () => {
    it('should return a promise from get method', () => {
      const instance = new DBrowser(testAppName)
      const result = instance.get('test-key')
      expect(result).toBeInstanceOf(Promise)
    })
  })

  describe('Instance properties', () => {
    it('should maintain different instances with different configurations', () => {
      const instance1 = new DBrowser('app1')
      const instance2 = new DBrowser('app2')
      const instance3 = new DBrowser('app3')

      expect(instance1).toBeInstanceOf(DBrowser)
      expect(instance2).toBeInstanceOf(DBrowser)
      expect(instance3).toBeInstanceOf(DBrowser)

      expect(instance1).not.toBe(instance2)
      expect(instance2).not.toBe(instance3)
    })
  })

  describe('Data storage and retrieval', () => {
    it('should store and retrieve an array of objects', async () => {
      const instance = new DBrowser(testAppName)
      const testData = [
        { id: 1, name: 'John', email: 'john@example.com' },
        { id: 2, name: 'Jane', email: 'jane@example.com' },
        { id: 3, name: 'Bob', email: 'bob@example.com' },
      ]
      const testKey = 'users-list'

      instance.set(testKey, testData)

      const retrieved = await instance.get(testKey)
      expect(retrieved).toEqual(testData)
      expect(Array.isArray(retrieved)).toBe(true)
      expect(retrieved).toHaveLength(3)
      expect((retrieved as typeof testData)[0]).toEqual({
        id: 1,
        name: 'John',
        email: 'john@example.com',
      })
    })

    it('should handle empty array storage and retrieval', async () => {
      const instance = new DBrowser(testAppName)
      const testData: unknown[] = []
      const testKey = 'empty-array'

      instance.set(testKey, testData)

      const retrieved = await instance.get(testKey)
      expect(retrieved).toEqual([])
      expect(Array.isArray(retrieved)).toBe(true)
      expect(retrieved).toHaveLength(0)
    })
  })
})
