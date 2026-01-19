import js from "@eslint/js"
import parser from "@typescript-eslint/parser"
import plugin from "@typescript-eslint/eslint-plugin"

export default [
  js.configs.recommended,
  {
    ignores: [".wrangler/**", "out/**"],
  },
  {
    files: ["**/*.ts"],
    languageOptions: {
      parser,
      parserOptions: { sourceType: "module" },
    },
    plugins: { "@typescript-eslint": plugin },
    rules: {
      "no-undef": "off",
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
    },
  },
]
