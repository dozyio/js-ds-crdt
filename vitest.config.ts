import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/**/*.spec.ts'],
    benchmark: {
      include: ['test/**/*.bench.ts']
    },
    globals: true,
  },
})
