module github.com/antonmedv/medb/bench

go 1.23

require github.com/antonmedv/medb v0.0.0-20260821135904-8bd674100da9

// The benchmark measures the working tree, not a published version, and is
// never installed on its own.
replace github.com/antonmedv/medb => ..
