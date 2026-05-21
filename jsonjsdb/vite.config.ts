import { defineConfig } from 'vitest/config'
import { playwright } from '@vitest/browser-playwright'
import dts from 'vite-plugin-dts'

export default defineConfig({
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: 'chromium', headless: true }],
      screenshotFailures: false,
    },
    silent: true,
  },
  plugins: [dts({ entryRoot: 'src', include: ['src'] })],
  build: {
    minify: 'terser',
    sourcemap: true,
    lib: {
      entry: './src/Jsonjsdb.ts',
      name: 'Jsonjsdb',
      formats: ['es', 'iife'],
      fileName: format => {
        if (format === 'iife') return 'jsonjsdb.min.js'
        return 'jsonjsdb.esm.js'
      },
    },
    rollupOptions: {
      external: ['crypto-js', 'localdata'],
      output: {
        globals: {
          'crypto-js': 'CryptoJS',
          localdata: 'localdata',
        },
      },
    },
  },
})
