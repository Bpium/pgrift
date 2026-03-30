_default:
	@just --list --unsorted

# Run pgrift migrate
migrate:
	npm run dev

publish:
	npm run build
	npm version patch
	npm publish
	git push --follow-tags

# Cleanup database schemas
clean:
	npm run cleanup

compare:
	npm run comparison

# Start migration in background via pm2
pm2-start:
	npm run pm2:start

# Stop pm2 process
pm2-stop:
	npm run pm2:stop

# Watch pm2 logs
pm2-logs:
	npm run pm2:logs

# Show pm2 status
pm2-status:
	npm run pm2:status

# Lint and format (Biome)
lint:
	npm run lint

lint-fix:
	npm run lint:fix

format:
	npm run format

parse:
	python3 scripts/parse.py

# Show available commands
help:
	@just --list
