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
  // the dedicated build tsconfig sets an explicit rootDir so declarations
  // land at dist/*.d.ts, where package.json points (checked by postbuild)
  plugins: [dts({ tsconfigPath: 'tsconfig.build.json' })],
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
  },
})
