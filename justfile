_default:
	@just --list --unsorted

# Run pgrift migrate
dev:
	npm run dev

# Cleanup database schemas
clean:
	npm run cleanup

# Show available commands
help:
	@just --list
