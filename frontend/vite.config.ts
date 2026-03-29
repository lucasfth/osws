import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'
import peggy from 'peggy'

function peggyPlugin(): Plugin {
  return {
    name: 'vite-peggy',
    transform(code, id) {
      if (!id.endsWith('.peggy')) return
      const src = peggy.generate(code, { output: 'source', format: 'es' })
      return { code: src, map: null }
    },
  }
}

// https://vite.dev/config/
export default defineConfig({
  server: {
    allowedHosts: [
      "together-malamute-finer.ngrok-free.app",
      "http://localhost:5173"
    ]
  },
  plugins: [
    peggyPlugin(),
    react({
      babel: {
        plugins: [['babel-plugin-react-compiler']],
      },
    }),
    tailwindcss(),
  ],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
})
