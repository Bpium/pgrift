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

# Show available commands
help:
	@just --list
