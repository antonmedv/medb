#!/usr/bin/env zx

// Measures coverage of a single Go module. The library and the CLI are separate
// modules, so each needs its own run:
//
//   zx .github/scripts/coverage.mjs .   80
//   zx .github/scripts/coverage.mjs cmd 80

const [dir, expected] = argv._
if (dir === undefined || expected === undefined) {
  echo(chalk.red('usage: coverage.mjs <dir> <expected>'))
  process.exit(2)
}

cd(path.resolve(__dirname, '..', '..', dir))

await spinner('Running tests', async () => {
  await $`go test -coverprofile=coverage.out -coverpkg=./... ./...`
  await $`go tool cover -html=coverage.out -o coverage.html`
})

const cover = await $({verbose: true})`go tool cover -func=coverage.out`
const total = +cover.stdout.match(/total:\s+\(statements\)\s+(\d+\.\d+)%/)[1]
if (total < expected) {
  echo(chalk.red(`Coverage is too low: ${total}% < ${expected}% (expected)`))
  process.exit(1)
} else {
  echo(`Coverage is good: ${chalk.green(total + '%')} >= ${expected}% (expected)`)
}
