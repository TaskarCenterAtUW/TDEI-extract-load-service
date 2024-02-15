/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  clearMocks: true,
  restoreMocks: true,
  coverageProvider: "v8",
  moduleFileExtensions: ["js", "jsx", "ts", "tsx", "json", "node"],
  // roots: ["<rootDir>"],
  // testMatch: ["**/test/*.ts", "**/?(*.)+(spec|test).[tj]s?(x)"],
  transform: {
    "^.+\\.(ts|tsx|js)$": "ts-jest",
  },
  reporters: [
    "default",
    ["./node_modules/jest-html-reporter", {
      "pageTitle": "Test Report",
      "includeFailureMsg": true
    }]
  ],
};