_default:
	@just --list --unsorted

# Run pgrift migrate
migrate:
	npm run dev

# Cleanup database schemas
clean:
	npm run cleanup

compare:
	npm run comparison

# Lint and format (Biome)
lint:
	npm run lint

lint-fix:
	npm run lint:fix

format:
	npm run format

# Show available commands
help:
	@just --list
