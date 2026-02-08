import js from '@eslint/js'
import tseslint from 'typescript-eslint'

const allowedProps = ['crypto-js', '__table__']

export default [
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    ignores: ['**/*.json.js', '**/dist/**', 'jsonjsdb-py/**'],
  },
  {
    files: ['**/*.{ts,tsx}'],
    ignores: ['eslint.config.ts'],
    rules: {
      '@typescript-eslint/naming-convention': [
        'error',
        {
          selector: 'variableLike',
          format: ['camelCase'],
        },
        {
          selector: 'function',
          format: ['camelCase'],
        },
        {
          selector: 'method',
          format: ['camelCase'],
        },
        {
          selector: 'class',
          format: ['PascalCase'],
        },
        {
          selector: 'interface',
          format: ['PascalCase'],
        },
        {
          selector: 'typeAlias',
          format: ['PascalCase'],
        },
        {
          selector: 'property',
          format: ['camelCase'],
          filter: {
            regex: `^(${allowedProps.join('|')})$`,
            match: false,
          },
        },
        {
          selector: 'property',
          filter: {
            regex: `^(${allowedProps.join('|')})$`,
            match: true,
          },
          format: null,
        },
      ],
    },
  },
]
