import js from "@eslint/js";
import typescript from "@typescript-eslint/eslint-plugin";
import typescriptParser from "@typescript-eslint/parser";
import unusedImports from "eslint-plugin-unused-imports";

export default [
	js.configs.recommended,
	{
		files: ["src/**/*.ts", "src/**/*.tsx"],
		languageOptions: {
			parser: typescriptParser,
			parserOptions: {
				ecmaVersion: 2020,
				sourceType: "module",
				project: "./tsconfig.json",
				tsconfigRootDir: import.meta.dirname,
			},
			globals: {
				console: "readonly",
				process: "readonly",
				Buffer: "readonly",
				__dirname: "readonly",
				setTimeout: "readonly",
				clearTimeout: "readonly",
				setInterval: "readonly",
				clearInterval: "readonly",
				Promise: "readonly",
			},
		},
		plugins: {
			"@typescript-eslint": typescript,
			"unused-imports": unusedImports,
		},
		rules: {
			// Error on unused variables
			"@typescript-eslint/no-unused-vars": [
				"error",
				{
					vars: "all",
					args: "after-used",
					ignoreRestSiblings: false,
					argsIgnorePattern: "^_",
					varsIgnorePattern: "^_",
					caughtErrors: "all",
				},
			],

			// Error on unused imports
			"unused-imports/no-unused-imports": "error",
			"unused-imports/no-unused-vars": [
				"error",
				{
					vars: "all",
					varsIgnorePattern: "^_",
					args: "after-used",
					argsIgnorePattern: "^_",
				},
			],

			// TypeScript specific rules
			"@typescript-eslint/explicit-module-boundary-types": "off",
			"@typescript-eslint/no-explicit-any": "off",
			"@typescript-eslint/no-empty-function": "warn",
			"no-unsafe-optional-chaining": "off",
			"no-case-declarations": "off",
			"no-prototype-builtins": "off",

			// General rules
			"no-console": "off",
			"no-unused-vars": "off", // Use TypeScript version instead
			"no-undef": "off", // TypeScript handles this
			"no-useless-catch": "off", // Allow try/catch blocks that only re-throw
			// Unexpected lexical declaration in case block.eslintno-case-declarations

			// ...typescript.configs.recommended.rules,
		},
	},
	{
		ignores: [
			"dist/**",
			"node_modules/**",
			"*.js",
			"!eslint.config.js",
			"test.ts",
		],
	},
];
