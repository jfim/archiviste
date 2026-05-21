default:
    @just --list

# Install dependencies
deps:
    mix deps.get

# Compile with warnings as errors
compile:
    mix compile --warnings-as-errors

# Format code
fmt:
    mix format

# Check formatting
fmt-check:
    mix format --check-formatted

# Run credo (linter)
credo:
    mix credo --strict

# Run dialyzer (static analysis)
dialyzer:
    mix dialyzer

# Run tests
test:
    mix test

# Run test coverage
coverage:
    mix coveralls

# Run all checks (CI equivalent)
check:
    mix check
