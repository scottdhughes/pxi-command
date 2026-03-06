import { execSync } from 'node:child_process'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const configDir = dirname(fileURLToPath(import.meta.url))

function resolveGitValue(command: string, fallback: string) {
  try {
    return execSync(command, {
      cwd: configDir,
      stdio: ['ignore', 'pipe', 'ignore'],
    }).toString().trim() || fallback
  } catch {
    return fallback
  }
}

function buildMetadataPlugin(): Plugin {
  return {
    name: 'pxi-build-metadata',
    generateBundle() {
      const metadata = {
        generated_at: new Date().toISOString(),
        commit_sha: process.env.CF_PAGES_COMMIT_SHA || resolveGitValue('git rev-parse HEAD', 'local-dev'),
        branch: process.env.CF_PAGES_BRANCH || resolveGitValue('git rev-parse --abbrev-ref HEAD', 'local'),
        pages_url: process.env.CF_PAGES_URL || null,
      }

      this.emitFile({
        type: 'asset',
        fileName: 'build.json',
        source: JSON.stringify(metadata, null, 2),
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), tailwindcss(), buildMetadataPlugin()],
})
